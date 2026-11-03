//! The node, Kubernetes API and stopped sandbox the stopped runtime upgrade
//! tests run against.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{Method, StatusCode};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;

use super::super::runtime_upgrade_dispatch::{E0, E1, block_e0_for_e1, publish_environment};
use super::super::*;
use crate::controllers::substrate::SubstrateAdapter;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, KubeReply, KubeRequest, ScriptedWorker, ScriptedWorkerConfig, WORKER_NODE,
    kube_status, scripted_kube_client_async, start_scripted_worker,
};
use crate::controllers::substrate::runtime_upgrade::StoppedRuntimeUpgrade;

pub(super) const UPGRADE_TIMEOUT: Duration = Duration::from_secs(3);

/// A Kubernetes API holding at most one sandbox pod.
#[derive(Default)]
pub(super) struct Pods {
    pub(super) pod: Mutex<Option<serde_json::Value>>,
    pub(super) creates: AtomicUsize,
    pub(super) deletes: AtomicUsize,
}

impl Pods {
    pub(super) fn exists(&self) -> bool {
        self.pod.lock().unwrap().is_some()
    }
}

pub(super) fn pods_client(pods: Arc<Pods>) -> kube::Client {
    let (client, _log) = scripted_kube_client_async(move |request: KubeRequest| {
        let pods = Arc::clone(&pods);
        async move {
            let pods_path = "/api/v1/namespaces/space/pods";
            match request.method {
                Method::GET if request.path.starts_with(pods_path) => {
                    match pods.pod.lock().unwrap().clone() {
                        Some(pod) => KubeReply::Json(StatusCode::OK, pod),
                        None => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                    }
                }
                Method::POST if request.path == pods_path => {
                    pods.creates.fetch_add(1, Ordering::SeqCst);
                    kube_status(
                        StatusCode::BAD_REQUEST,
                        "the stopped runtime upgrade places no pod yet",
                    )
                }
                Method::DELETE if request.path.starts_with(pods_path) => {
                    pods.deletes.fetch_add(1, Ordering::SeqCst);
                    match pods.pod.lock().unwrap().take() {
                        Some(_) => kube_status(StatusCode::OK, ""),
                        None => kube_status(StatusCode::NOT_FOUND, "NotFound"),
                    }
                }
                method => kube_status(
                    StatusCode::BAD_REQUEST,
                    &format!("unexpected Kubernetes request {method} {}", request.path),
                ),
            }
        }
    });
    client
}

/// A stopped sandbox whose disk is on `node-a` and whose last execution E0
/// ran, with E0 blocked in favor of the default E1 along a certified edge.
pub(super) struct Subject {
    pub(super) db: pplx_space_db_test_fixtures::TestDb,
    pub(super) sandbox_id: SandboxId,
    pub(super) node_id: pplx_space_common_core::types::NodeId,
    pub(super) substrate: KubernetesSubstrate,
    pub(super) worker: ScriptedWorker,
    pub(super) pods: Arc<Pods>,
}

pub(super) async fn subject() -> Result<Subject, BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&pool, sandbox_id, "stopped").await?;
    let node_id = pplx_space_common_svc::types::k8s::space_k8s_node_id(WORKER_NODE, "worker");
    pplx_space_db_test_fixtures::insert_ready_worker_node(&pool, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&pool)
    .await?;
    // The execution the sandbox stopped from, on the node holding its disk.
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-stopped", None, false).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = 'node-a' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    publish_environment(&pool, sandbox_id, node_id, E0).await?;
    block_e0_for_e1(&pool, &[E1]).await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let pods = Arc::new(Pods::default());
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        pool,
        pods_client(Arc::clone(&pods)),
        worker.port,
        None,
    );
    Ok(Subject {
        db,
        sandbox_id,
        node_id,
        substrate,
        worker,
        pods,
    })
}

impl Subject {
    /// A substrate over the same database, node and pods, whose policy cache
    /// is empty, as another replica's would be.
    pub(super) fn forget_cached_policy(&mut self) {
        self.substrate = substrate_with_store_client_proxy_port_and_pod(
            self.db.pool.clone(),
            pods_client(Arc::clone(&self.pods)),
            self.worker.port,
            None,
        );
    }

    pub(super) async fn upgrade(&self) -> Result<StoppedRuntimeUpgrade, SubstrateError> {
        self.substrate
            .upgrade_stopped_runtime(self.sandbox_id, UPGRADE_TIMEOUT)
            .await
    }

    pub(super) async fn lifecycle(&self) -> Result<String, BoxError> {
        Ok(
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(self.sandbox_id)
                .fetch_one(&self.db.pool)
                .await?,
        )
    }

    /// The active execution's generation, attempt target and whether its
    /// lease still runs.
    pub(super) async fn active_execution(
        &self,
    ) -> Result<Option<(i64, Option<String>, bool)>, BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT generation, activation_attempt_target_state,
                    COALESCE(activation_attempt_expires_at > NOW(), FALSE)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_optional(&self.db.pool)
        .await?)
    }

    /// The upgrade the active execution records: operation, target and
    /// authorization.
    pub(super) async fn recorded_upgrade(
        &self,
    ) -> Result<(Option<uuid::Uuid>, Option<String>, Option<String>), BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT runtime_upgrade_operation_id, runtime_upgrade_target_environment_ref,
                    runtime_upgrade_authorization_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_one(&self.db.pool)
        .await?)
    }

    /// No pod was created and nothing was sent to the node.
    pub(super) fn assert_nothing_placed_or_sent(&self, context: &str) {
        assert_eq!(self.pods.creates.load(Ordering::SeqCst), 0, "{context}");
        for method in [
            "WorkerApiInternal/WakeSandbox",
            "WorkerApi/EnqueueSandboxArtifactBackup",
        ] {
            assert!(
                self.worker.first_call(method).is_none(),
                "{context}: the upgrade called {method}"
            );
        }
    }

    /// The row is stopped, and no execution or pod is left.
    pub(super) async fn assert_stopped_without_execution(
        &self,
        context: &str,
    ) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert_eq!(self.active_execution().await?, None, "{context}");
        assert!(!self.pods.exists(), "{context}: a pod outlived the upgrade");
        Ok(())
    }

    /// The node published the sandbox stopped under E1.
    pub(super) async fn publish_upgraded(&self) -> Result<(), BoxError> {
        publish_environment(&self.db.pool, self.sandbox_id, self.node_id, E1).await
    }
}

/// The refusal an upgrade the node can't boot yet answers with.
pub(super) fn assert_not_available_yet(outcome: Result<StoppedRuntimeUpgrade, SubstrateError>) {
    match outcome {
        Err(SubstrateError::Unsupported { message }) => {
            assert!(message.contains("runtime-upgrade pod mode"), "{message}");
        }
        other => panic!("expected the upgrade to be unsupported for now, got {other:?}"),
    }
}
