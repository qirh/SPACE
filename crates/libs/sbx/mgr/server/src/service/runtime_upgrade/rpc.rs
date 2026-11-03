//! The `UpgradeRuntime` and `GetRuntimeUpgrade` RPCs.
//!
//! On Kubernetes a sandbox's microVM launches in its pod's shim at container
//! create, so a runtime upgrade runs there too: the pod's `runtime-upgrade`
//! checkpoint makes the shim call `UpgradeRuntime` with the cold start it
//! would otherwise send as a `Start`, carrying the pod's network namespace
//! and route identity. That caller can't do anything else afterwards, so the
//! RPC does the whole of what the pod needs under one upgrade lock: stage a
//! suspended sandbox placed away from its checkpoint, upgrade, and for a
//! started target hand back the running execution, activating a sandbox that
//! was already on the target. The control plane can't see the response, so it
//! reads the outcome from `GetRuntimeUpgrade`.

use tonic::metadata::MetadataMap;
use tonic::{Extensions, Request, Response, Status};

use super::{
    RuntimeUpgradeOutcome, RuntimeUpgradeRequest, parse_operation_id, parse_request,
    parse_sandbox_id,
};
use crate::db::{RuntimeUpgradePhase, RuntimeUpgradeStatus, RuntimeUpgradeTargetState, SandboxRow};
use crate::proto::{
    GetRuntimeUpgradeRequest, GetRuntimeUpgradeResponse, RuntimeUpgradeStageFrom, StartRequest,
    StartResponse, UpgradeRuntimeRequest, UpgradeRuntimeResponse,
};
use crate::state::SandboxState;

#[cfg(test)]
mod tests;

/// The staging `Start` for a suspended sandbox absent from the node: the
/// upgrade's own start, as a restore from `stage_from`'s checkpoint. A blank
/// storage URI is refused by the staging `Start`'s own parsing.
fn staging_start(start: &StartRequest, stage_from: &RuntimeUpgradeStageFrom) -> StartRequest {
    StartRequest {
        sandbox_id: start.sandbox_id.clone(),
        vcpu: start.vcpu,
        memory_mib: start.memory_mib,
        checkpoint: Some(crate::service::create_support::SUSPEND_CHECKPOINT.to_string()),
        blocking: false,
        netns_path: start.netns_path.clone(),
        suspend_restore_storage_uri: Some(stage_from.suspend_restore_storage_uri.clone()),
        suspend_restore_source_node: stage_from.suspend_restore_source_node.clone(),
        route_identity: start.route_identity.clone(),
        activation_auto_lifecycle: start.activation_auto_lifecycle.clone(),
        operation_activation_attempt_id: start.operation_activation_attempt_id.clone(),
        ..Default::default()
    }
}

/// Whether `start` names the pod whose execution `sandbox` runs: the same pod
/// UID and execution generation. Both must be present; an identity without
/// them names no pod.
fn names_the_running_pod(sandbox: &SandboxRow, start: &StartRequest) -> bool {
    let running = crate::service::activation_support::space_api_route_identity_to_wire(
        &sandbox.space_api_route_identity,
    );
    let (Some(running), Some(requested)) = (running.as_ref(), start.route_identity.as_ref()) else {
        return false;
    };
    running.pod_uid.is_some()
        && running.execution_generation.is_some()
        && running.pod_uid == requested.pod_uid
        && running.execution_generation == requested.execution_generation
}

impl crate::service::SandboxManagerService {
    pub(in crate::service) async fn upgrade_runtime(
        &self,
        request: Request<UpgradeRuntimeRequest>,
    ) -> Result<Response<UpgradeRuntimeResponse>, Status> {
        self.authorize_peer(
            &request,
            "UpgradeRuntime",
            crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
        )?;
        let (metadata, extensions, mut message) = request.into_parts();
        let stage_from = message.stage_from.take();
        let upgrade = parse_request(message)?;
        let sandbox_id = upgrade.sandbox_id.clone();
        let _guard = self.runtime_upgrade_guard(&sandbox_id).await?;
        // Boxed: staging, the upgrade and an activation together are a large
        // future, which would otherwise sit on the stack of an unoptimized
        // build's RPC task.
        let response = Box::pin(self.upgrade_runtime_locked(
            metadata,
            extensions,
            upgrade,
            stage_from.as_ref(),
        ))
        .await;
        crate::node_state_sync::mark_sandbox_dirty(&self.db, &sandbox_id);
        response.map(Response::new)
    }

    /// The RPC's work under the sandbox's upgrade lock.
    async fn upgrade_runtime_locked(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        upgrade: RuntimeUpgradeRequest,
        stage_from: Option<&RuntimeUpgradeStageFrom>,
    ) -> Result<UpgradeRuntimeResponse, Status> {
        if let Some(stage_from) = stage_from {
            // Leaves a present sandbox alone, in any state; the upgrade's
            // checkpoint check decides whether its copy may be upgraded.
            self.stage_missing_checkpoint(staging_start(&upgrade.start, stage_from))
                .await?;
        }
        let target_state = upgrade.target_state;
        let operation_id = upgrade.operation_id;
        let start = upgrade.start.clone();
        let outcome = self
            .caller_upgrader(metadata.clone(), extensions.clone())
            .upgrade(upgrade)
            .await?;
        let wire_outcome = match outcome {
            RuntimeUpgradeOutcome::Upgraded { .. } | RuntimeUpgradeOutcome::Replayed => {
                crate::proto::RuntimeUpgradeOutcome::Upgraded
            }
            RuntimeUpgradeOutcome::AlreadyOnTarget => {
                crate::proto::RuntimeUpgradeOutcome::AlreadyOnTarget
            }
        };
        let start = match (outcome, target_state) {
            (RuntimeUpgradeOutcome::Upgraded { start }, _) => Some(*start),
            (_, RuntimeUpgradeTargetState::Stopped) => None,
            // A replay never moves the sandbox again, so it can only report
            // the execution its boot left running, to the pod that runs it.
            (RuntimeUpgradeOutcome::Replayed, RuntimeUpgradeTargetState::Started) => {
                let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
                Some(self.running_execution_for(&sandbox, &start, || {
                    format!(
                        "runtime upgrade {operation_id} already finished and sandbox {} is {}, \
                         not running this pod's execution",
                        sandbox.id, sandbox.state
                    )
                })?)
            }
            (RuntimeUpgradeOutcome::AlreadyOnTarget, RuntimeUpgradeTargetState::Started) => {
                Some(Box::pin(self.activate_once(metadata, extensions, operation_id, start)).await?)
            }
        };
        Ok(UpgradeRuntimeResponse {
            outcome: wire_outcome as i32,
            start,
        })
    }

    /// Activate a sandbox the upgrade found already on its target, at most
    /// once per operation: the start is claimed durably before it runs, so a
    /// retry after it, even one after a crash mid-start, is answered as a
    /// replay is, with the live execution of the same pod or a refusal. A
    /// start that failed is never tried again under the same operation.
    async fn activate_once(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        operation_id: crate::db::RuntimeUpgradeOperationId,
        start: StartRequest,
    ) -> Result<StartResponse, Status> {
        let claimed = crate::db::read::runtime_upgrade_report(&*self.db_conn()?, operation_id)
            .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?
            .is_some_and(|report| {
                report.status
                    == RuntimeUpgradeStatus::AlreadyOnTarget {
                        activation_claimed: true,
                    }
            });
        if claimed {
            let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
            return self.running_execution_for(&sandbox, &start, || {
                format!(
                    "runtime upgrade {operation_id} already tried to start sandbox {}, which is \
                     {} and not running this pod's execution",
                    sandbox.id, sandbox.state
                )
            });
        }
        let claimed = crate::db::claim_runtime_upgrade_activation(
            &*self.db_conn()?,
            operation_id,
            &start.sandbox_id,
        )
        .map_err(|error| Status::internal(format!("claim runtime upgrade activation: {error}")))?;
        if !claimed {
            return Err(Status::aborted(format!(
                "runtime upgrade {operation_id} has no recorded outcome to claim its activation on"
            )));
        }
        self.activate_on_target(metadata, extensions, start).await
    }

    /// Activate a sandbox the upgrade found already on its target, with the
    /// upgrade's start, so the pod that asked runs it: a stopped sandbox cold
    /// starts, a suspended one restores its checkpoint (the target's), and a
    /// started one is this pod's only if it runs this pod's execution.
    async fn activate_on_target(
        &self,
        metadata: MetadataMap,
        extensions: Extensions,
        mut start: StartRequest,
    ) -> Result<StartResponse, Status> {
        let sandbox = self.lookup_sandbox(&start.sandbox_id)?;
        match sandbox.state {
            SandboxState::Stopped => {}
            SandboxState::Suspended => {
                start.checkpoint =
                    Some(crate::service::create_support::SUSPEND_CHECKPOINT.to_string());
            }
            _ => {
                return self.running_execution_for(&sandbox, &start, || {
                    format!(
                        "sandbox {} is already on the runtime upgrade's target and is {}, \
                         not running this pod's execution",
                        sandbox.id, sandbox.state
                    )
                });
            }
        }
        // The upgrade's caller holds Start's authority: the upgrade's
        // principals are a subset of Start's.
        self.start_with_runtime_upgrade(Request::from_parts(metadata, extensions, start), None)
            .await
            .map(Response::into_inner)
    }

    /// The live execution of `sandbox` when it's started and its route
    /// identity names the pod `start` came from. Anything else is refused
    /// with `refusal`, rather than moved.
    fn running_execution_for(
        &self,
        sandbox: &SandboxRow,
        start: &StartRequest,
        refusal: impl FnOnce() -> String,
    ) -> Result<StartResponse, Status> {
        if sandbox.state != SandboxState::Started || !names_the_running_pod(sandbox, start) {
            return Err(Status::failed_precondition(refusal()));
        }
        crate::service::create_support::checkpoint_start_response_for_live_runtime(
            sandbox,
            (self.sandbox_runtime_live)(&sandbox.id),
        )
        .ok_or_else(|| Status::failed_precondition(refusal()))
    }

    pub(in crate::service) fn get_runtime_upgrade(
        &self,
        request: &Request<GetRuntimeUpgradeRequest>,
    ) -> Result<GetRuntimeUpgradeResponse, Status> {
        use crate::proto::RuntimeUpgradePhase as WirePhase;
        use crate::proto::RuntimeUpgradeStatus as Wire;
        self.authorize_peer(
            request,
            "GetRuntimeUpgrade",
            crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let operation_id = parse_operation_id(&request.get_ref().operation_id)?;
        let report = crate::db::read::runtime_upgrade_report(&*self.db_conn()?, operation_id)
            .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?;
        let Some(report) = report else {
            return Ok(GetRuntimeUpgradeResponse {
                status: Wire::Absent as i32,
                ..Default::default()
            });
        };
        if report.sandbox_id != sandbox_id {
            return Err(Status::invalid_argument(format!(
                "runtime upgrade {operation_id} names another sandbox"
            )));
        }
        let mut response = GetRuntimeUpgradeResponse::default();
        response.status = match report.status {
            RuntimeUpgradeStatus::InProgress(phase) => {
                response.phase = match phase {
                    RuntimeUpgradePhase::Fenced => WirePhase::Fenced,
                    RuntimeUpgradePhase::Preserved => WirePhase::Preserved,
                    RuntimeUpgradePhase::Booting => WirePhase::Booting,
                    RuntimeUpgradePhase::Verified => WirePhase::Verified,
                    // The report names these Upgraded and RolledBack.
                    RuntimeUpgradePhase::RolledBack | RuntimeUpgradePhase::Retired => {
                        WirePhase::Unspecified
                    }
                } as i32;
                Wire::InProgress
            }
            RuntimeUpgradeStatus::Upgraded => Wire::Upgraded,
            RuntimeUpgradeStatus::RolledBack { failure } => {
                response.failure = failure;
                Wire::RolledBack
            }
            RuntimeUpgradeStatus::AlreadyOnTarget { .. } => Wire::AlreadyOnTarget,
        } as i32;
        Ok(response)
    }
}
