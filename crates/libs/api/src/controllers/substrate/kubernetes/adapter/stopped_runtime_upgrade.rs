//! Upgrading a stopped sandbox's runtime without starting it
//! ([`SubstrateAdapter::upgrade_stopped_runtime`]).
//!
//! The start-time decision ([`KubernetesSubstrate::decide_runtime_upgrade`])
//! chooses the move and names the operation, as for a start. The sandbox's
//! row stays `stopped` throughout:
//!
//! 1. **Claim.** An activation attempt targeting `stopped` on the sandbox's
//!    active execution, which nothing else claims on a `stopped` row.
//!    Activation-completion receipts release only a `created`, `started` or
//!    `paused` target, so the claim never moves the row, and a user start
//!    waits for the attempt like any other.
//! 2. **Record** the upgrade (operation, target, authorization) on the
//!    claimed execution, so a retry repeats exactly that upgrade.
//! 3. **Refuse.** Booting the disk under the target needs a node-side pod
//!    mode whose container create runs the upgrade with the pod's network
//!    namespace, which doesn't exist yet. Until it lands the operation
//!    answers [`SubstrateError::Unsupported`] (the stopped-sandbox runtime
//!    upgrade row of the gap table in `kubernetes/mod.rs`), places no pod and
//!    sends nothing to a node, and expires the claim, keeping its target and
//!    the recorded upgrade.
//!
//! A retry that finds an execution an earlier attempt left takes it over
//! first, then reads what it recorded, so nothing can be recorded between the
//! two. An execution claimed with nothing recorded is retired, and the retry
//! decides afresh; one with a recorded upgrade keeps that upgrade, whatever
//! the policy says now, so the boot the node-side mode adds finishes the
//! operation an earlier attempt may have started. `retire_upgrade_boot` and
//! `enqueue_upgraded_disk_backup` are the steps that follow that
//! boot: back up the upgraded disk, then delete the pod and finish the
//! execution.

use super::super::runtime_upgrade::DecidedUpgrade;
use super::super::store::{ActivationFenceOwnership, RecordedRuntimeUpgrade};
use super::*;
use crate::controllers::substrate::execution_fence::{
    ExecutionIdentity, ObservedPod, RecordedExecution,
};
use crate::controllers::substrate::runtime_upgrade::{StoppedRuntimeUpgrade, UpgradeActivation};
use pplx_space_common_core::types::SandboxActivationAttemptId;

/// The generation and attempt of an execution an upgrade claimed.
type Claimed = (i64, SandboxActivationAttemptId);

/// What an attempt records on its execution for the upgrade `decided` names.
fn recorded_upgrade(decided: DecidedUpgrade) -> Result<RecordedRuntimeUpgrade, SubstrateError> {
    let authorization_id = decided
        .wake
        .authorization
        .as_ref()
        .map(|authorization| authorization.authorization_id.clone())
        .ok_or_else(|| SubstrateError::Failed {
            message: "the decided runtime upgrade names no authorization".to_string(),
        })?;
    Ok(RecordedRuntimeUpgrade {
        operation_id: decided.operation_id,
        target: decided.movement.to,
        authorization_id,
    })
}

impl KubernetesSubstrate {
    /// `_timeout` will bound the boot the node-side runtime-upgrade pod mode
    /// adds; nothing waits on a node yet.
    pub(super) async fn upgrade_stopped_runtime_on_kubernetes(
        &self,
        sandbox_id: SandboxId,
        _timeout: Duration,
    ) -> Result<StoppedRuntimeUpgrade, SubstrateError> {
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        if normalize_lifecycle(&sandbox.lifecycle) != "stopped" {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "a runtime upgrade without a start needs a stopped sandbox, not {}",
                    sandbox.lifecycle
                ),
            });
        }
        let left = self.store.stopped_runtime_upgrade_claim(sandbox_id).await?;
        if left.as_ref().is_some_and(|claim| claim.live) {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "another runtime upgrade of stopped sandbox {sandbox_id} is in flight; retry"
                ),
            });
        }
        let upgrade = match left {
            // An earlier attempt left its execution. Take it over first, so a
            // start or another retry that claims it meanwhile either wins,
            // which fails the takeover, or waits for this one; then read what
            // it recorded, which nothing else can record once this attempt
            // owns it.
            Some(claim) => {
                let takeover = self
                    .store
                    .take_over_stopped_runtime_upgrade(
                        sandbox_id,
                        claim.generation,
                        claim.activation_attempt_id,
                        SandboxActivationAttemptId::new(),
                    )
                    .await
                    .map_err(|error| match error {
                        SubstrateError::ActivationInFlight { .. } => SubstrateError::Unavailable {
                            message: format!(
                                "another activation owns stopped sandbox {sandbox_id}; retry its \
                                 runtime upgrade"
                            ),
                        },
                        error => error,
                    })?;
                let recorded = self
                    .store
                    .stopped_runtime_upgrade_claim(sandbox_id)
                    .await?
                    .filter(|current| {
                        current.generation == claim.generation
                            && current.activation_attempt_id == takeover
                    })
                    .ok_or_else(|| SubstrateError::Unavailable {
                        message: format!(
                            "stopped sandbox {sandbox_id}'s execution changed hands during its \
                             runtime upgrade's takeover; retry"
                        ),
                    })?
                    .upgrade;
                match recorded {
                    // It recorded this upgrade and didn't finish it. Keep it,
                    // whatever the policy says now, so the node finishes or
                    // replays the operation it may have begun.
                    Some(recorded) => {
                        self.store
                            .expire_activation_attempt_best_effort(
                                sandbox_id,
                                claim.generation,
                                takeover,
                            )
                            .await;
                        Some(recorded)
                    }
                    // It recorded nothing, so no node was asked for anything.
                    None => {
                        self.retire_upgrade_boot(sandbox_id, claim.generation, takeover, false)
                            .await?;
                        self.plan_upgrade(sandbox_id).await?
                    }
                }
            }
            None => self.plan_upgrade(sandbox_id).await?,
        };
        let Some(upgrade) = upgrade else {
            return Ok(StoppedRuntimeUpgrade::AsRecorded);
        };
        // A stopped sandbox whose disk is only in object storage has no node
        // to boot it on; the upgrade doesn't stage one.
        let source = self.store.restore_source_plan(sandbox_id).await?;
        if source.stopped_evacuation_unplaced {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id} is moving off a draining node; retry its runtime \
                     upgrade once it's placed"
                ),
            });
        }
        if source.node_name.is_none() {
            return Err(SubstrateError::FailedPrecondition {
                message: format!(
                    "stopped sandbox {sandbox_id}'s disk isn't on a node, so its runtime can't be \
                     upgraded until it is started"
                ),
            });
        }

        let claimed = self
            .claim_and_record_upgrade(sandbox_id, SandboxActivationAttemptId::new(), &upgrade)
            .await;
        let (generation, attempt_id) = match claimed {
            Ok(claim) => claim,
            Err((error, None)) => return Err(error),
            Err((error, Some((generation, attempt_id)))) => {
                self.store
                    .expire_activation_attempt_best_effort(sandbox_id, generation, attempt_id)
                    .await;
                return Err(error);
            }
        };
        // The boot lands with the node-side runtime-upgrade pod mode. Until
        // then nothing was placed or sent, so the claim only needs to end;
        // the recorded upgrade stays for the retry.
        self.store
            .expire_activation_attempt_best_effort(sandbox_id, generation, attempt_id)
            .await;
        tracing::info!(
            sandbox_id = %sandbox_id,
            operation_id = %upgrade.operation_id,
            to = %upgrade.target,
            "recorded the stopped sandbox's runtime upgrade; the node can't boot it yet"
        );
        Err(SubstrateError::Unsupported {
            message: format!(
                "stopped sandbox {sandbox_id}'s runtime upgrade {} to {} is recorded, but the \
                 node-side runtime-upgrade pod mode that boots it isn't available yet; the \
                 sandbox stays stopped on its environment",
                upgrade.operation_id, upgrade.target
            ),
        })
    }

    /// The upgrade the start-time decision chooses, or `None` to stay on the
    /// recorded environment.
    async fn plan_upgrade(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RecordedRuntimeUpgrade>, SubstrateError> {
        self.decide_runtime_upgrade(sandbox_id, UpgradeActivation::Start, None)
            .await?
            .map(recorded_upgrade)
            .transpose()
    }

    /// Claim the sandbox's active execution with `requested` targeting
    /// `stopped`, and record `upgrade` on it. Answers the claimed generation
    /// and attempt, or the error with the claim to expire when one was taken.
    async fn claim_and_record_upgrade(
        &self,
        sandbox_id: SandboxId,
        requested: SandboxActivationAttemptId,
        upgrade: &RecordedRuntimeUpgrade,
    ) -> Result<Claimed, (SubstrateError, Option<Claimed>)> {
        let changed_hands = || SubstrateError::Unavailable {
            message: format!(
                "stopped sandbox {sandbox_id}'s execution changed hands before its runtime \
                 upgrade; retry"
            ),
        };
        let claim = self
            .store
            .ensure_active_execution_claim_for_lifecycle(
                sandbox_id,
                SandboxLifecycle::Stopped,
                None,
                Some((requested, SandboxTransitionTargetState::Stopped)),
            )
            .await
            .map_err(|error| match error {
                // Another activation owns the execution: a user start, or
                // this upgrade on another replica.
                SubstrateError::ActivationInFlight { .. } => SubstrateError::Unavailable {
                    message: format!(
                        "another activation owns stopped sandbox {sandbox_id}; retry its runtime \
                         upgrade"
                    ),
                },
                error => error,
            })
            .map_err(|error| (error, None))?;
        let generation = claim.row.generation;
        let Some(attempt_id) = claim.row.activation_attempt_id else {
            return Err((changed_hands(), None));
        };
        let claimed = Some((generation, attempt_id));
        let fence = self
            .store
            .reconcile_activation_fence(
                sandbox_id,
                attempt_id,
                None,
                SandboxTransitionTargetState::Stopped,
            )
            .await
            .map_err(|error| (error, claimed))?;
        // Only an attempt this call owns may record the upgrade.
        if fence.ownership() != ActivationFenceOwnership::Owned(attempt_id)
            || fence.execution.generation != generation
        {
            return Err((changed_hands(), claimed));
        }
        let recorded = self
            .store
            .record_stopped_runtime_upgrade(sandbox_id, generation, attempt_id, upgrade)
            .await
            .map_err(|error| (error, claimed))?;
        if !recorded {
            return Err((changed_hands(), claimed));
        }
        Ok((generation, attempt_id))
    }

    /// Delete the pod the upgrade booted and finish its execution. With
    /// `backup`, the upgraded disk's current-instance backup is enqueued first,
    /// so the durable copy matches the environment the node published; a
    /// failed enqueue keeps the execution for a retry. Until the node-side
    /// boot lands, only a takeover of a claim that recorded nothing retires,
    /// without a backup.
    pub(in crate::controllers::substrate::kubernetes) async fn retire_upgrade_boot(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        attempt_id: SandboxActivationAttemptId,
        backup: bool,
    ) -> Result<(), SubstrateError> {
        let active = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .filter(|execution| execution.generation == generation);
        // Another attempt adopted the execution, and its pod with it, or it
        // never materialized.
        let Some(execution) =
            active.filter(|execution| execution.activation_attempt_id == Some(attempt_id))
        else {
            // The upgrade isn't done while its backup is unconfirmed.
            if backup {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "stopped sandbox {sandbox_id}'s upgraded execution left this attempt \
                         before its disk backup was enqueued; retry"
                    ),
                });
            }
            return Ok(());
        };
        let pod_name = parse_pod_name(&execution.pod_name)?;
        // An attempt can end between creating the pod and recording its UID.
        // The pod's name is this generation's, so the live pod identifies it.
        let pod_uid = match execution.pod_uid.clone() {
            Some(pod_uid) => Some(pod_uid),
            None => fetch_sandbox_pod_uncached(&self.k8s, pod_name.as_str())
                .await
                .map_err(|error| SubstrateError::Failed {
                    message: format!("failed to read the upgrade's pod: {error}"),
                })?
                .and_then(|pod| pod.uid().and_then(|uid| PodUid::parse(uid).ok())),
        };
        if backup {
            self.enqueue_upgraded_disk_backup(sandbox_id, execution.pod_ip.as_deref())
                .await?;
        }
        if let Some(pod_uid) = &pod_uid {
            self.delete_upgrade_pod(&pod_name, pod_uid).await?;
        }
        let identity = ExecutionIdentity {
            pod_name,
            generation: Some(execution.generation),
            activation_attempt_id: execution.activation_attempt_id,
            recorded: RecordedExecution::Read {
                pod_uid: execution.pod_uid.clone(),
                route_activation_id: execution.route_activation_id,
            },
            pod: ObservedPod::Absent,
        };
        // Generation- and attempt-guarded: a successor keeps its execution.
        // An execution that changed since it was read stays active, with what
        // it recorded, so this retire isn't done.
        if !self
            .store
            .forget_fenced_pod_tracking(sandbox_id, &identity, PlacementClock::Preserve)
            .await?
        {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s runtime upgrade execution changed before it \
                     was retired; retry"
                ),
            });
        }
        Ok(())
    }

    /// Enqueue the backup of the stopped sandbox's current instance through
    /// the upgrade's pod, as the stop path does before it releases a pod. Any
    /// failure is retryable: the durable copy may still be the pre-upgrade
    /// disk.
    async fn enqueue_upgraded_disk_backup(
        &self,
        sandbox_id: SandboxId,
        pod_ip: Option<&str>,
    ) -> Result<(), SubstrateError> {
        let unavailable = |reason: String| SubstrateError::Unavailable {
            message: format!(
                "stopped sandbox {sandbox_id} is upgraded, but its disk backup isn't enqueued: \
                 {reason}; retry"
            ),
        };
        let pod_ip = pod_ip
            .and_then(|ip| PodIp::parse(ip).ok())
            .ok_or_else(|| unavailable("the upgrade's pod has no address".to_string()))?;
        let mut client = Self::pod_worker_client(
            pod_ip,
            self.k8s.sandbox_proxy_port,
            self.budgets.teardown_worker_connect,
        )
        .await
        .ok_or_else(|| unavailable("the pod's node-api is unreachable".to_string()))?;
        let endpoint_addr = pod_ip.socket_addr(self.k8s.sandbox_proxy_port).to_string();
        enqueue_artifact_backup_with_client(
            &mut client,
            &endpoint_addr,
            sandbox_id,
            ArtifactKind::Disk,
            BackupSource::CurrentInstance,
        )
        .await
        .map(drop)
        .map_err(|error| unavailable(error.to_string()))
    }

    /// Delete the pod at `pod_name`, but only while it's the one with `uid`.
    async fn delete_upgrade_pod(
        &self,
        pod_name: &PodName,
        uid: &PodUid,
    ) -> Result<(), SubstrateError> {
        delete_sandbox_pod_fenced(&self.k8s, pod_name, uid, None)
            .await
            .map(drop)
            .map_err(|error| SubstrateError::Failed {
                message: format!("failed to delete the upgrade's pod: {error}"),
            })
    }
}

fn parse_pod_name(name: &str) -> Result<PodName, SubstrateError> {
    PodName::parse(name).map_err(|error| SubstrateError::Failed {
        message: format!("upgrade execution has an invalid pod name: {error}"),
    })
}
