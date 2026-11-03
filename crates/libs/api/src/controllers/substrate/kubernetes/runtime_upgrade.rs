//! The resume- and start-time runtime upgrade decision, on Kubernetes.
//!
//! [`KubernetesSubstrate::activation_runtime_upgrade`] reads the policy, the
//! sandbox's recorded execution and the environment catalog, and turns
//! [`decide`]'s plan into the `WakeRequest.runtime_upgrade` the activation's
//! Wake carries. Placement doesn't change: a restore pod placed on the node
//! that holds the sandbox's checkpoint upgrades it there, and one placed
//! elsewhere receives the checkpoint's storage URI in the Wake's start context,
//! from which the node stages the disk before it upgrades it. A stopped
//! sandbox's pod is bound to the node holding its disk, as for any start.

use super::KubernetesSubstrate;
use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::runtime_upgrade::{
    EnvironmentMove, RuntimePlan, UpgradeActivation, attribute, decide, stranded_error,
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;

impl KubernetesSubstrate {
    /// The runtime upgrade an activation of `sandbox_id` asks the node for,
    /// or `None` to start or restore as recorded. A stranded sandbox is
    /// refused. `restore_storage_uri` is the restore's selected checkpoint,
    /// when it has one.
    pub(super) async fn activation_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<Option<pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade>, SubstrateError> {
        if !self.k8s.runtime_upgrade_at_activation.is_enabled() {
            return Ok(None);
        }
        Ok(self
            .decide_runtime_upgrade(sandbox_id, activation, restore_storage_uri)
            .await?
            .map(|decided| decided.wake))
    }

    /// [`Self::activation_runtime_upgrade`]'s decision, whatever the
    /// activation setting: the move and the upgrade that makes it, or `None`
    /// to stay on the recorded environment. A stranded sandbox is refused.
    pub(super) async fn decide_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<Option<DecidedUpgrade>, SubstrateError> {
        let policy = self
            .runtime_policy
            .get(|| self.store.runtime_environment_policy())
            .await?;
        // Without a default the policy narrows nothing, so no evidence is
        // needed.
        if policy.default_environment.is_none() {
            record(sandbox_id, activation, RuntimePlan::AsRecorded);
            return Ok(None);
        }
        let recorded = match activation {
            UpgradeActivation::Restore => {
                self.store
                    .restore_checkpoint_execution(sandbox_id, restore_storage_uri)
                    .await?
            }
            UpgradeActivation::Start => self.store.last_published_execution(sandbox_id).await?,
        };
        // A resume's upgrade boots the node's copy of the checkpoint only if
        // the node can tell it's this one, by its local artifact; a checkpoint
        // without that identity is left to the node's own restore.
        let Some(recorded) = recorded.filter(|recorded| {
            activation == UpgradeActivation::Start || recorded.local_artifact_id.is_some()
        }) else {
            return Ok(unattributed(sandbox_id, activation));
        };
        // A bound record on an allowed environment stays whatever the catalog
        // says, so the common case reads no catalog.
        if let Some(bound) = recorded
            .execution
            .environment
            .as_deref()
            .and_then(|environment| environment.parse().ok())
            && policy.environments.get(&bound) == Some(&RuntimeEnvironmentState::Allowed)
        {
            record(sandbox_id, activation, RuntimePlan::AsRecorded);
            return Ok(None);
        }
        let catalog = self.store.unretired_runtime_environments().await?;
        let Some(from) = attribute(&recorded.execution, &catalog, policy.legacy_unbound_allowed)
        else {
            return Ok(unattributed(sandbox_id, activation));
        };
        let from_catalog = catalog
            .iter()
            .find(|environment| environment.reference == from);
        let plan = match decide(&policy, activation, from, from_catalog) {
            // The cached policy may be up to its TTL old. Before asking the
            // node to discard a checkpoint's memory, decide again on the
            // policy as it is now: an environment allowed again in the
            // meantime restores as recorded.
            RuntimePlan::ColdBootUpgrade(to_default)
                if to_default.authorization(activation).memory_loss =>
            {
                let current = self.store.runtime_environment_policy().await?;
                decide(&current, activation, from, from_catalog)
            }
            plan => plan,
        };
        record(sandbox_id, activation, plan);
        match plan {
            RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => Ok(None),
            RuntimePlan::ColdBootUpgrade(to_default) => {
                let wake = to_default.wake_upgrade(
                    sandbox_id,
                    activation,
                    &recorded.subject,
                    recorded.local_artifact_id,
                );
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    operation_id = %wake.operation_id,
                    "waking the sandbox by its runtime upgrade"
                );
                Ok(Some(DecidedUpgrade {
                    movement: to_default,
                    operation_id: to_default.operation_id(
                        sandbox_id,
                        activation,
                        &recorded.subject,
                    ),
                    wake,
                }))
            }
            RuntimePlan::Stranded(to_default) => Err(stranded_error(sandbox_id, to_default)),
        }
    }
}

/// A cold-boot upgrade [`KubernetesSubstrate::decide_runtime_upgrade`] chose.
pub(super) struct DecidedUpgrade {
    pub(super) movement: EnvironmentMove,
    /// The operation the upgrade runs as; `wake` carries it too.
    pub(super) operation_id: uuid::Uuid,
    /// The `WakeRequest.runtime_upgrade` that asks the node for it.
    pub(super) wake: pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade,
}

/// No recorded execution attributes the sandbox to a catalog environment. The
/// node's execution policy still refuses a runtime that may not execute; this
/// only means the control plane can't move it first.
fn unattributed<T>(sandbox_id: SandboxId, activation: UpgradeActivation) -> Option<T> {
    tracing::info!(
        sandbox_id = %sandbox_id,
        activation = activation.as_str(),
        "no recorded execution attributes this sandbox to a catalog environment; \
         activating as recorded"
    );
    pplx_space_metrics::runtime_upgrade::activation_decision(
        activation.as_str(),
        "unattributed",
        "none",
    );
    None
}

/// Count and log one decision.
fn record(sandbox_id: SandboxId, activation: UpgradeActivation, plan: RuntimePlan) {
    let to_default = match plan {
        RuntimePlan::AsRecorded => None,
        RuntimePlan::RestoreOnDefault(to_default)
        | RuntimePlan::ColdBootUpgrade(to_default)
        | RuntimePlan::Stranded(to_default) => Some(to_default),
    };
    let cause = to_default.map_or("none", |to_default| to_default.cause.as_str());
    pplx_space_metrics::runtime_upgrade::activation_decision(
        activation.as_str(),
        plan.as_str(),
        cause,
    );
    if let Some(to_default) = to_default {
        tracing::info!(
            sandbox_id = %sandbox_id,
            activation = activation.as_str(),
            plan = plan.as_str(),
            from = %to_default.from,
            to = %to_default.to,
            cause,
            "decided the sandbox's runtime at activation"
        );
    }
}
