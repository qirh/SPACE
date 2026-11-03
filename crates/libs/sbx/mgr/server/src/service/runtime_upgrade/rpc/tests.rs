//! `UpgradeRuntime` and `GetRuntimeUpgrade` as a Kubernetes pod's shim calls
//! them: with the pod's network namespace and its Kubernetes route identity,
//! never a Direct one, against a test-served node-manager.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{ExecutionMetadata, LocalArtifactId, SandboxId};
use pplx_space_node_mgr_client::proto::{
    CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotRequest,
    DeleteBtrfsSnapshotResponse, ResolveExecutionEnvironmentRequest,
    ResolveExecutionEnvironmentResponse, RestoreInstanceSnapshotRequest,
    RestoreInstanceSnapshotResponse,
};
use tonic::Status;

use super::super::node_effects::tests::{
    Fixture, certified_target, environments, fixture, recording,
};
use crate::db::{RuntimeUpgradeOperationId, RuntimeUpgradePhase};
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::{
    GetRuntimeUpgradeRequest, RuntimeUpgradeAuthorization, RuntimeUpgradeOutcome,
    RuntimeUpgradeStatus, SandboxSubstrateKind, SpaceApiRouteIdentity, StartRequest,
    UpgradeRuntimeRequest, WakeTargetState,
};
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::state::SandboxState;

const POD_NETNS: &str = "/var/run/netns/cni-runtime-upgrade";
const POD_UID: &str = "4f6b0d4e-6a59-4b5c-9e43-3a1d0d6f2a10";
const OTHER_POD_UID: &str = "9d2c6f1b-0c1e-4a57-8b0e-5f7a3c2d1e40";

/// The route identity a pod's shim sends: Kubernetes, with the pod's UID.
fn pod_identity(sandbox_id: &str, pod_uid: &str, generation: i64) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        sandbox_id: Some(sandbox_id.to_string()),
        route_generation: Some(generation.cast_unsigned()),
        current_node_id: Some(crate::service::test_options::test_node_id().to_string()),
        substrate_kind: SandboxSubstrateKind::Kubernetes as i32,
        execution_generation: Some(generation),
        pod_uid: Some(pod_uid.to_string()),
        activation_attempt_id: None,
    }
}

/// The `UpgradeRuntime` a runtime upgrade pod's shim sends: the pod's own
/// cold start, with its namespace and route identity.
fn pod_upgrade(
    sandbox_id: &str,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
    target_state: WakeTargetState,
) -> UpgradeRuntimeRequest {
    UpgradeRuntimeRequest {
        sandbox_id: sandbox_id.to_string(),
        operation_id: operation_id.to_string(),
        target_environment: target.environment().unwrap().to_string(),
        authorization: Some(authorization()),
        start: Some(StartRequest {
            sandbox_id: sandbox_id.to_string(),
            blocking: true,
            netns_path: Some(POD_NETNS.to_string()),
            route_identity: Some(pod_identity(sandbox_id, POD_UID, 4)),
            ..Default::default()
        }),
        target_state: target_state as i32,
        stage_from: None,
        checkpoint_local_artifact_id: None,
    }
}

fn operation() -> RuntimeUpgradeOperationId {
    RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7())
}

fn authorization() -> RuntimeUpgradeAuthorization {
    RuntimeUpgradeAuthorization {
        authorization_id: "environment-block-e0".into(),
        reason: "e0 runs a blocked runtime".into(),
        memory_loss: true,
    }
}

impl Fixture {
    fn assert_no_node_calls(&self) {
        assert!(self._node.calls().is_empty(), "{:?}", self._node.calls());
    }

    /// Save `target` as the sandbox's consumer, as a sandbox already on it.
    fn put_on(&self, target: &ExecutionMetadata) {
        crate::db::upsert_sandbox_execution_metadata(
            &self.service.db.lock().unwrap(),
            &self.sandbox_id,
            target,
        )
        .unwrap();
    }

    /// The fixture's service, seeing every runtime as live.
    fn live_service(&self) -> SandboxManagerService {
        SandboxManagerService {
            sandbox_runtime_live: Arc::new(|_| true),
            ..self.service.clone()
        }
    }
}

async fn upgrade(
    service: &SandboxManagerService,
    request: UpgradeRuntimeRequest,
) -> Result<crate::proto::UpgradeRuntimeResponse, Status> {
    service
        .upgrade_runtime(internal_request(request))
        .await
        .map(tonic::Response::into_inner)
}

fn report(
    service: &SandboxManagerService,
    sandbox_id: &str,
    operation_id: RuntimeUpgradeOperationId,
) -> crate::proto::GetRuntimeUpgradeResponse {
    service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: sandbox_id.to_string(),
            operation_id: operation_id.to_string(),
        }))
        .unwrap()
}

/// A node-manager that answers the upgrade's disk copy and the target's
/// resolution, so the upgrade reaches its cold boot, and records the disk
/// restore a rollback from the boot phase performs.
fn node_reaching_the_boot(
    target: &ExecutionMetadata,
    calls: &Arc<Mutex<Vec<String>>>,
) -> FakeNodeManager {
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let compiled =
        CompiledEnvironment::for_architecture(pplx_space_common_core::types::Arch::X86_64).unwrap();
    let definition = compiled
        .iter()
        .find(|environment| Some(environment.definition().reference()) == target.environment())
        .unwrap()
        .definition()
        .clone();
    let execution = definition
        .bind_execution(definition.execution_requirements().unwrap())
        .unwrap();
    let resolved = ResolveExecutionEnvironmentResponse {
        accepted_selection: 0,
        definition: Some((&definition).into()),
        execution: Some((&execution).into()),
    };
    let (deleted, created, restored) = (calls.clone(), calls.clone(), calls.clone());
    FakeNodeManager::default()
        .unary(
            "DeleteBtrfsSnapshot",
            move |request: DeleteBtrfsSnapshotRequest| {
                recording(&deleted, format!("delete {}", request.snapshot_name));
                Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
            },
        )
        .unary(
            "CreateBtrfsSnapshot",
            move |request: CreateBtrfsSnapshotRequest| {
                recording(&created, format!("create {}", request.snapshot_name));
                Ok(CreateBtrfsSnapshotResponse {})
            },
        )
        .unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| Ok(resolved.clone()),
        )
        .unary(
            "RestoreInstanceSnapshot",
            move |request: RestoreInstanceSnapshotRequest| {
                recording(&restored, format!("restore {}", request.snapshot_name));
                Ok(RestoreInstanceSnapshotResponse {})
            },
        )
}

/// The upgrade's boot runs with the pod's namespace: a pod's cold start gets
/// past the runtime-context check a namespace-less Kubernetes Wake fails, and
/// the upgrade reaches its boot. This harness can't launch a microVM, so the
/// boot fails and the upgrade rolls back from its boot phase (restoring the
/// preserved disk), leaving the old pair and the rollback as the operation's
/// recorded outcome.
#[tokio::test]
async fn a_pods_upgrade_boots_in_the_pods_namespace_under_the_target() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (source, _) = environments();
    let target = certified_target(&source);
    let f = fixture(
        SandboxState::Stopped,
        false,
        node_reaching_the_boot(&target, &calls),
    );
    let operation_id = operation();

    let error = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .expect_err("the harness can't boot a sandbox");

    assert!(
        !error
            .message()
            .contains(pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER),
        "{error}"
    );
    assert!(error.message().contains("rolled back"), "{error}");
    assert!(error.message().contains("cold boot failed"), "{error}");
    let preserved = super::super::preserved_disk_name(operation_id);
    assert!(
        calls
            .lock()
            .unwrap()
            .contains(&format!("restore {preserved}")),
        "the rollback must restore the disk the boot may have written: {:?}",
        calls.lock().unwrap()
    );
    let row = f.row();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(
        row.execution_metadata
            .as_ref()
            .and_then(ExecutionMetadata::environment),
        f.source.environment()
    );
    let reported = report(&f.service, &f.sandbox_id, operation_id);
    assert_eq!(reported.status, RuntimeUpgradeStatus::RolledBack as i32);
    assert!(
        reported
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("cold boot failed")),
        "{reported:?}"
    );
}

/// A sandbox already on the target fences nothing, records that, and leaves a
/// stopped target as it is. Asked again under the same operation, the node
/// answers the same without touching the disk.
#[tokio::test]
async fn an_upgrade_already_on_its_target_is_recorded_and_replays_the_same() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::Absent as i32
    );

    for _ in 0..2 {
        let response = upgrade(
            &f.service,
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Stopped,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            response.outcome,
            RuntimeUpgradeOutcome::AlreadyOnTarget as i32
        );
        assert_eq!(response.start, None);
        assert_eq!(
            report(&f.service, &f.sandbox_id, operation_id).status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32
        );
    }
    f.assert_no_node_calls();
    assert_eq!(f.row().state, SandboxState::Stopped);
    // An operation names one sandbox.
    let other = SandboxId::new().to_string();
    let refused = f
        .service
        .get_runtime_upgrade(&internal_request(GetRuntimeUpgradeRequest {
            sandbox_id: other,
            operation_id: operation_id.to_string(),
        }))
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
}

/// A started target on a stopped sandbox already on it is the pod's
/// activation: the same `UpgradeRuntime` cold-starts it with the pod's start,
/// rather than answering with nothing and leaving the shim to issue a second
/// RPC. This harness can't launch, so the start's own failure is what the
/// call returns, after the upgrade recorded it found its target.
#[tokio::test]
async fn a_started_target_already_reached_is_activated_in_place() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    let plain = f
        .service
        .start_with_runtime_upgrade(internal_request(request.start.clone().unwrap()), None)
        .await
        .expect_err("the harness can't boot a sandbox");

    let activated = upgrade(&f.service, request)
        .await
        .expect_err("the harness can't boot a sandbox");

    // The same start, so the same refusal.
    assert_eq!(activated.code(), plain.code(), "{activated} / {plain}");
    assert_eq!(activated.message(), plain.message());
    // The start was claimed before it ran, so a retry of the operation never
    // tries it again.
    let retried = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(retried.code(), tonic::Code::FailedPrecondition, "{retried}");
    assert!(
        retried.message().contains("already tried to start"),
        "{retried}"
    );
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
    assert!(
        !f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot") || call.contains("InstanceSnapshot")),
        "{:?}",
        f._node.calls()
    );
}

/// Give the fixture's sandbox a live started execution with route
/// `identity`, as a verified upgrade boot leaves it.
fn run_as(f: &Fixture, identity: &SpaceApiRouteIdentity) {
    let db = f.service.db.lock().unwrap();
    crate::db::update_runtime_fields(
        &db,
        f.sandbox_id.parse().unwrap(),
        crate::db::RuntimeFields {
            uid: 10000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10001),
            netgtw_pid: 10002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity:
                crate::service::activation_support::space_api_route_identity_from_wire(Some(
                    identity,
                ))
                .unwrap(),
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();
    db.execute(
        "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
        crate::db::fixture::params![f.sandbox_id],
    )
    .unwrap();
}

/// Record a retired upgrade of the fixture's sandbox under `operation_id`.
fn retire(
    f: &Fixture,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
    target_state: crate::db::RuntimeUpgradeTargetState,
) {
    let db = f.service.db.lock().unwrap();
    let sandbox = crate::db::read::sandbox(&db, &f.sandbox_id)
        .unwrap()
        .unwrap();
    let begun = crate::db::begin_runtime_upgrade(
        &db,
        crate::db::NewRuntimeUpgrade {
            operation_id,
            sandbox_id: &f.sandbox_id,
            observed: crate::db::ObservedSandbox::of(&sandbox),
            target_environment: target.environment().unwrap(),
            target_state,
            authorization: &crate::db::RuntimeUpgradeAuthorization {
                id: "environment-block-e0".into(),
                reason: "e0 runs a blocked runtime".into(),
                memory_loss: true,
            },
            checkpoint_dir_identity: None,
            previous_execution: &f.source,
        },
    )
    .unwrap();
    assert!(
        matches!(begun, crate::db::BeginRuntimeUpgrade::Fenced(_)),
        "{begun:?}"
    );
    assert!(
        crate::db::advance_runtime_upgrade(
            &db,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(crate::db::bind_runtime_upgrade_consumer(&db, operation_id, target).unwrap());
    for (from, to) in [
        (RuntimeUpgradePhase::Booting, RuntimeUpgradePhase::Verified),
        (RuntimeUpgradePhase::Verified, RuntimeUpgradePhase::Retired),
    ] {
        assert!(crate::db::advance_runtime_upgrade(&db, operation_id, from, to).unwrap());
    }
}

/// Who runs the sandbox when a finished upgrade is replayed.
#[derive(Clone, Copy, Debug)]
enum Runner {
    /// Nothing: the sandbox is stopped, as the upgrade left it.
    Nobody,
    /// The pod with this UID, started.
    Pod(&'static str),
    /// The replaying pod's UID at an earlier execution generation.
    EarlierGeneration,
    /// The replaying pod's execution, stopped since.
    StoppedSince,
    /// A started execution with no pod identity, replayed by a request with
    /// none either: an identity without a pod names no pod.
    Unidentified,
}

/// A replay of an upgrade that already finished never moves the sandbox
/// again. For a started target it reports the execution the boot left
/// running, but only to the pod that runs it: a fresh pod replaying the same
/// operation is refused cleanly, so the control plane re-places it, rather
/// than handed another pod's execution. A stopped target answers upgraded
/// with nothing to run.
#[tokio::test]
async fn a_replay_of_a_finished_upgrade_answers_only_the_pod_that_runs_it() {
    use crate::db::RuntimeUpgradeTargetState::{Started, Stopped};
    for (target_state, runner, expect) in [
        (Started, Runner::Pod(POD_UID), Ok(true)),
        (Started, Runner::Pod(OTHER_POD_UID), Err(())),
        (Started, Runner::EarlierGeneration, Err(())),
        (Started, Runner::StoppedSince, Err(())),
        (Started, Runner::Nobody, Err(())),
        (Started, Runner::Unidentified, Err(())),
        (Stopped, Runner::Nobody, Ok(false)),
    ] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let operation_id = operation();
        retire(&f, &target, operation_id, target_state);
        let wire_state = match target_state {
            Started => WakeTargetState::Started,
            Stopped => WakeTargetState::Stopped,
        };
        let mut request = pod_upgrade(&f.sandbox_id, &target, operation_id, wire_state);
        match runner {
            Runner::Nobody => {}
            Runner::Pod(pod_uid) => run_as(&f, &pod_identity(&f.sandbox_id, pod_uid, 4)),
            Runner::EarlierGeneration => run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 3)),
            Runner::StoppedSince => {
                run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
                f.service
                    .db
                    .lock()
                    .unwrap()
                    .execute(
                        "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                        crate::db::fixture::params![f.sandbox_id],
                    )
                    .unwrap();
            }
            Runner::Unidentified => {
                let mut identity = pod_identity(&f.sandbox_id, POD_UID, 4);
                identity.pod_uid = None;
                identity.substrate_kind = SandboxSubstrateKind::Unspecified as i32;
                run_as(&f, &identity);
                request.start.as_mut().unwrap().route_identity = Some(identity);
            }
        }
        let service = f.live_service();
        let before = f.row();

        let outcome = upgrade(&service, request).await;

        match (expect, outcome) {
            (Ok(attached), Ok(response)) => {
                assert_eq!(response.outcome, RuntimeUpgradeOutcome::Upgraded as i32);
                assert_eq!(
                    response.start.as_ref().map(|start| start.pid),
                    attached.then(std::process::id),
                    "{target_state:?} {runner:?}"
                );
            }
            (Err(()), Err(error)) => {
                assert_eq!(
                    error.code(),
                    tonic::Code::FailedPrecondition,
                    "{runner:?}: {error}"
                );
                assert!(error.message().contains("already finished"), "{error}");
            }
            (expect, outcome) => {
                panic!("{target_state:?} {runner:?}: expected {expect:?}, got {outcome:?}")
            }
        }
        let after = f.row();
        assert_eq!(
            after.state, before.state,
            "a replay must not move the sandbox"
        );
        assert_eq!(after.node_execution_id, before.node_execution_id);
        assert_eq!(
            report(&f.service, &f.sandbox_id, operation_id).status,
            RuntimeUpgradeStatus::Upgraded as i32
        );
        f.assert_no_node_calls();
    }
}

/// A started sandbox already on the target is the pod's only if it runs the
/// pod's execution: the same pod gets its execution back, any other pod is
/// refused, and nothing is stopped or started either way.
#[tokio::test]
async fn a_started_sandbox_already_on_its_target_answers_only_its_own_pod() {
    for (runner, attached) in [(POD_UID, true), (OTHER_POD_UID, false)] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        f.put_on(&target);
        run_as(&f, &pod_identity(&f.sandbox_id, runner, 4));
        let service = f.live_service();
        let operation_id = operation();

        let outcome = upgrade(
            &service,
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Started,
            ),
        )
        .await;

        if attached {
            let response = outcome.unwrap();
            assert_eq!(
                response.outcome,
                RuntimeUpgradeOutcome::AlreadyOnTarget as i32
            );
            assert_eq!(
                response.start.map(|start| start.pid),
                Some(std::process::id())
            );
        } else {
            let error = outcome.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(
                error
                    .message()
                    .contains("already on the runtime upgrade's target"),
                "{error}"
            );
        }
        assert_eq!(f.row().state, SandboxState::Started);
        assert_eq!(
            report(&f.service, &f.sandbox_id, operation_id).status,
            RuntimeUpgradeStatus::AlreadyOnTarget as i32
        );
        f.assert_no_node_calls();
    }
}

/// A suspended sandbox whose checkpoint the target produced is activated by
/// restoring that checkpoint, with the pod's start: the `Start` a restore
/// pod sends, keeping the sandbox's memory.
#[tokio::test]
async fn a_suspended_sandbox_already_on_its_target_is_restored_in_place() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    std::fs::write(
        f.checkpoint().join("runtime.json"),
        serde_json::to_vec(
            &pplx_space_common_core::types::SnapshotExecutionMetadata::new(target.clone()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = f.adopt_checkpoint();
    let operation_id = operation();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    let activated = upgrade(&f.service, request)
        .await
        .expect_err("the harness can't restore a sandbox");

    // The checkpoint restore's own stage-in is what this harness can't do;
    // a cold start of the suspended row would fail elsewhere.
    assert!(
        activated
            .message()
            .starts_with("suspend snapshot stage-in failed"),
        "{activated}"
    );
    assert_eq!(
        report(&f.service, &f.sandbox_id, operation_id).status,
        RuntimeUpgradeStatus::AlreadyOnTarget as i32
    );
}

/// A suspended sandbox absent from the node is staged from `stage_from`
/// before the upgrade looks for it. This harness has no object store, so
/// staging is what fails; without `stage_from` the upgrade finds no sandbox.
#[tokio::test]
async fn an_absent_suspended_sandbox_is_staged_before_the_upgrade() {
    let (source, _) = environments();
    let target = certified_target(&source);
    let service = SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test(),
    );
    let sandbox_id = SandboxId::new().to_string();
    let request = || {
        let mut request = pod_upgrade(&sandbox_id, &target, operation(), WakeTargetState::Started);
        request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
        request
    };

    let unstaged = upgrade(&service, request()).await.unwrap_err();
    assert_eq!(unstaged.code(), tonic::Code::NotFound, "{unstaged}");

    let mut staged = request();
    staged.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{sandbox_id}/{}?region=us-east-1",
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });
    let error = upgrade(&service, staged).await.unwrap_err();
    assert_ne!(error.code(), tonic::Code::NotFound, "{error}");
    assert!(error.message().contains("stage"), "{error}");
}

/// A suspended sandbox is upgraded only from the checkpoint the upgrade was
/// decided from, as the Wake's upgrade is: a copy pinning another artifact,
/// or an upgrade naming none, is refused before anything is fenced.
#[tokio::test]
async fn a_pods_upgrade_of_a_suspended_sandbox_needs_the_decided_checkpoint() {
    // (named checkpoint, whether the row pins a checkpoint of its own)
    for (decided, pinned) in [
        (Some(LocalArtifactId::new()), true),
        (None, true),
        (None, false),
    ] {
        let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
        if pinned {
            f.adopt_checkpoint();
        }
        let target = certified_target(&f.source);
        let mut request = pod_upgrade(
            &f.sandbox_id,
            &target,
            operation(),
            WakeTargetState::Started,
        );
        request.checkpoint_local_artifact_id = decided.map(|id| id.to_string());

        let error = upgrade(&f.service, request).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("decided from"), "{error}");
        assert_eq!(f.row().state, SandboxState::Suspended);
        f.assert_no_node_calls();
    }

    // The decided checkpoint passes the check and reaches the upgrade's node
    // work, which this fake doesn't answer.
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let checkpoint = f.adopt_checkpoint();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &certified_target(&f.source),
        operation(),
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(checkpoint.to_string());
    let error = upgrade(&f.service, request).await.unwrap_err();
    assert!(
        f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot")),
        "{error}: {:?}",
        f._node.calls()
    );
}

/// `stage_from` stages only a sandbox absent from the node; with a row here
/// it is ignored, and the upgrade goes on to the row the node holds.
#[tokio::test]
async fn staging_leaves_a_present_sandbox_to_the_upgrade() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation(),
        WakeTargetState::Stopped,
    );
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: "s3://space-test/suspend-snapshots/current?region=us-east-1"
            .into(),
        suspend_restore_source_node: Some("worker-source".into()),
    });

    let response = upgrade(&f.service, request).await.unwrap();

    assert_eq!(
        response.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );
    f.assert_no_node_calls();

    let mut malformed = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation(),
        WakeTargetState::Stopped,
    );
    malformed.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: " ".into(),
        suspend_restore_source_node: None,
    });
    let refused = upgrade(&f.service, malformed).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
}

/// A Kubernetes sandbox launches in its pod's shim, which never sees a Wake,
/// so a Wake carrying a runtime upgrade with a Kubernetes route identity is
/// refused before anything is staged, fenced or booted, with or without a
/// namespace. A Direct identity keeps its Wake path.
#[tokio::test]
async fn a_kubernetes_wake_refuses_a_runtime_upgrade() {
    for netns_path in [None, Some(POD_NETNS.to_string())] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        let request = crate::proto::WakeRequest {
            sandbox_id: f.sandbox_id.clone(),
            blocking: true,
            start_context: Some(crate::proto::WakeStartContext {
                route_identity: Some(pod_identity(&f.sandbox_id, POD_UID, 4)),
                netns_path,
                ..Default::default()
            }),
            runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
                operation_id: operation().to_string(),
                target_environment: target.environment().unwrap().to_string(),
                authorization: Some(authorization()),
                checkpoint_local_artifact_id: None,
            }),
            ..Default::default()
        };

        let error = f.service.wake(internal_request(request)).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("never from a Wake"), "{error}");
        assert_eq!(f.row().state, SandboxState::Stopped);
        f.assert_no_node_calls();
        assert_eq!(
            crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
                .unwrap(),
            None
        );
    }
}

/// The sandbox's own row decides too: a Wake that names a Direct identity
/// for a sandbox whose row records a Kubernetes pod is still refused.
#[tokio::test]
async fn a_wake_for_a_kubernetes_row_refuses_a_runtime_upgrade_whatever_it_claims() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let request = crate::proto::WakeRequest {
        sandbox_id: f.sandbox_id.clone(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(direct),
            ..Default::default()
        }),
        runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
            operation_id: operation().to_string(),
            target_environment: target.environment().unwrap().to_string(),
            authorization: Some(authorization()),
            checkpoint_local_artifact_id: None,
        }),
        ..Default::default()
    };

    let error = f.service.wake(internal_request(request)).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("never from a Wake"), "{error}");
    f.assert_no_node_calls();
}

/// What a shim that predates the runtime-upgrade checkpoint sends for an
/// upgrade pod: the checkpoint passed through as a `Start` checkpoint, with
/// the pod's namespace and identity. sandbox-manager refuses it for a stopped
/// row and for a suspended one, without staging or launching anything, so an
/// old shim fails closed whatever the row.
#[tokio::test]
async fn an_old_shims_runtime_upgrade_start_is_refused_on_a_stopped_or_suspended_row() {
    for state in [SandboxState::Stopped, SandboxState::Suspended] {
        let f = fixture(state, false, FakeNodeManager::default());
        let before = f.row();
        let error = f
            .service
            .start_with_runtime_upgrade(
                internal_request(StartRequest {
                    sandbox_id: f.sandbox_id.clone(),
                    checkpoint: Some("runtime-upgrade".to_string()),
                    netns_path: Some(POD_NETNS.to_string()),
                    suspend_restore_storage_uri: Some(
                        "s3://space-test/suspend-snapshots/current?region=us-east-1".into(),
                    ),
                    route_identity: Some(pod_identity(&f.sandbox_id, POD_UID, 4)),
                    ..Default::default()
                }),
                None,
            )
            .await
            .expect_err("an unknown checkpoint must not start or restore the sandbox");

        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{state}: {error}"
        );
        assert!(
            error.message().contains("runtime-upgrade"),
            "{state}: {error}"
        );
        let after = f.row();
        assert_eq!(after.state, state);
        assert_eq!(after.pid, None, "nothing may have launched");
        assert_eq!(
            after.current_local_artifact_id,
            before.current_local_artifact_id
        );
        f.assert_no_node_calls();
    }
}

/// A stage interrupted after it adopted the row leaves it `restoring`. A
/// retry with `stage_from` resumes that stage instead of handing the upgrade
/// a row it refuses. This harness has no object store, so the resumed stage
/// is what fails.
#[tokio::test]
async fn an_interrupted_stage_is_resumed_before_the_upgrade() {
    let f = fixture(SandboxState::Restoring, false, FakeNodeManager::default());
    {
        // A remote stage that adopted the row and recorded its metadata
        // staging, then stopped.
        let db = f.service.db.lock().unwrap();
        db.execute(
            "UPDATE sandboxes
                SET restore_origin = 'remote',
                    restore_phase = 'metadata_staging',
                    restore_storage_uri = ?2
              WHERE id = ?1",
            crate::db::fixture::params![
                f.sandbox_id,
                format!("s3://space-test/suspend/{}?region=us-east-1", f.sandbox_id)
            ],
        )
        .unwrap();
        assert!(crate::db::record_remote_restore_artifact_staging(&db, &f.sandbox_id).unwrap());
    }
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &certified_target(&f.source),
        operation(),
        WakeTargetState::Started,
    );
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());
    request.stage_from = Some(crate::proto::RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: format!(
            "s3://space-test/suspend/{}/{}?region=us-east-1",
            f.sandbox_id,
            uuid::Uuid::now_v7()
        ),
        suspend_restore_source_node: None,
    });

    let error = upgrade(&f.service, request).await.unwrap_err();

    assert!(
        error
            .message()
            .contains("resume interrupted cross-node restore staging"),
        "{error}"
    );
    assert_eq!(f.row().state, SandboxState::Restoring);
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
            .unwrap(),
        None
    );
}

/// An operation that claimed the start of a sandbox it found on its target
/// never starts it again: a retry finds it stopped since and is refused, and
/// the pod whose execution still runs gets that execution back.
#[tokio::test]
async fn an_activated_upgrade_never_starts_its_sandbox_again() {
    for running in [false, true] {
        let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
        let target = certified_target(&f.source);
        f.put_on(&target);
        let operation_id = operation();
        {
            let db = f.service.db.lock().unwrap();
            crate::db::record_runtime_upgrade_already_on_target(
                &db,
                &crate::db::RuntimeUpgradeRequestIdentity {
                    operation_id,
                    sandbox_id: f.sandbox_id.clone(),
                    target_environment: target.environment().unwrap(),
                    target_state: crate::db::RuntimeUpgradeTargetState::Started,
                    authorization_id: authorization().authorization_id,
                },
            )
            .unwrap();
            assert!(
                crate::db::claim_runtime_upgrade_activation(&db, operation_id, &f.sandbox_id)
                    .unwrap()
            );
        }
        if running {
            run_as(&f, &pod_identity(&f.sandbox_id, POD_UID, 4));
        }

        let outcome = upgrade(
            &f.live_service(),
            pod_upgrade(
                &f.sandbox_id,
                &target,
                operation_id,
                WakeTargetState::Started,
            ),
        )
        .await;

        if running {
            assert_eq!(
                outcome.unwrap().start.map(|start| start.pid),
                Some(std::process::id())
            );
        } else {
            let error = outcome.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(
                error.message().contains("already tried to start"),
                "{error}"
            );
            assert_eq!(f.row().state, SandboxState::Stopped);
        }
        f.assert_no_node_calls();
    }
}

/// A Wake replaying an upgrade that already retired must still name the
/// checkpoint the node holds: the replay itself moves nothing, and the Wake
/// goes on to restore whatever the row pins. A Direct sandbox that suspended
/// again since pins a newer checkpoint, and a replay naming the older one is
/// refused before anything is restored.
#[tokio::test]
async fn a_wake_replay_of_a_retired_upgrade_never_restores_a_newer_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Started,
    );
    // The sandbox suspended again since, to a checkpoint the node now pins.
    f.adopt_checkpoint();
    let mut direct = pod_identity(&f.sandbox_id, POD_UID, 4);
    direct.pod_uid = None;
    direct.substrate_kind = SandboxSubstrateKind::Direct as i32;
    let request = crate::proto::WakeRequest {
        sandbox_id: f.sandbox_id.clone(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(direct),
            ..Default::default()
        }),
        runtime_upgrade: Some(crate::proto::WakeRuntimeUpgrade {
            operation_id: operation_id.to_string(),
            target_environment: target.environment().unwrap().to_string(),
            authorization: Some(authorization()),
            checkpoint_local_artifact_id: Some(LocalArtifactId::new().to_string()),
        }),
        ..Default::default()
    };

    let error = f.service.wake(internal_request(request)).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("decided from"), "{error}");
    assert_eq!(f.row().state, SandboxState::Suspended);
    f.assert_no_node_calls();
}

/// A retry under an operation the node recorded an outcome for must ask for
/// what the operation asked for: one that found its sandbox on the target for
/// a stopped lifecycle can't come back as a start of it.
#[tokio::test]
async fn a_recorded_operation_cant_be_retried_as_another() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    f.put_on(&target);
    let operation_id = operation();
    let stopped = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Stopped,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        stopped.outcome,
        RuntimeUpgradeOutcome::AlreadyOnTarget as i32
    );

    let started = upgrade(
        &f.service,
        pod_upgrade(
            &f.sandbox_id,
            &target,
            operation_id,
            WakeTargetState::Started,
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(started.code(), tonic::Code::InvalidArgument, "{started}");
    assert_eq!(f.row().state, SandboxState::Stopped);
    f.assert_no_node_calls();
}

/// A replay that restores nothing answers whatever checkpoint the sandbox
/// holds since: a stopped-target upgrade that retired is still upgraded after
/// the sandbox suspended to a newer checkpoint.
#[tokio::test]
async fn a_stopped_target_replay_ignores_a_newer_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let target = certified_target(&f.source);
    let operation_id = operation();
    retire(
        &f,
        &target,
        operation_id,
        crate::db::RuntimeUpgradeTargetState::Stopped,
    );
    // The sandbox suspended again since, to a checkpoint the node now pins,
    // newer than the one the upgrade was decided from.
    f.adopt_checkpoint();
    let mut request = pod_upgrade(
        &f.sandbox_id,
        &target,
        operation_id,
        WakeTargetState::Stopped,
    );
    request.checkpoint_local_artifact_id = Some(LocalArtifactId::new().to_string());

    let response = upgrade(&f.service, request).await.unwrap();

    assert_eq!(response.outcome, RuntimeUpgradeOutcome::Upgraded as i32);
    assert_eq!(response.start, None);
    assert_eq!(f.row().state, SandboxState::Suspended);
    f.assert_no_node_calls();
}
