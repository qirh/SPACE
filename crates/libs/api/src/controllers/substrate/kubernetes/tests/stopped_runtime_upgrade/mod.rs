//! Upgrading a stopped sandbox's runtime without starting it, against
//! Postgres, a scripted node-api and a Kubernetes API. Until the node-side
//! runtime-upgrade pod mode lands, the operation decides, claims and records
//! the upgrade, then answers `Unsupported`: the row stays `stopped`, no pod is
//! placed and nothing reaches the node. Kubernetes only: Direct refuses the
//! operation, under the stopped-sandbox runtime upgrade row of the
//! `direct.rs` gap table.

use pplx_space_common_core::sql;

use super::runtime_upgrade_dispatch::{E0, E1};
use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::BoxError;
use crate::controllers::substrate::runtime_upgrade::StoppedRuntimeUpgrade;

mod fixture;
use fixture::*;

/// The decision chooses E1, the operation claims and records it, and the
/// node can't boot it yet: the operation refuses, the row stays stopped,
/// nothing is placed or sent, and the claim's lease is over. A retry takes
/// the same execution over and refuses the same way.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_the_node_cannot_boot_yet_is_recorded_and_refused() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let decided = s
        .substrate
        .decide_runtime_upgrade(
            s.sandbox_id,
            crate::controllers::substrate::runtime_upgrade::UpgradeActivation::Start,
            None,
        )
        .await?
        .expect("the start decision");

    assert_not_available_yet(s.upgrade().await);

    assert_eq!(s.lifecycle().await?, "stopped");
    s.assert_nothing_placed_or_sent("after the refusal");
    assert!(!s.pods.exists());
    let (generation, target, live) = s.active_execution().await?.expect("the claim");
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(!live, "the refused upgrade kept its claim's lease");
    // The same upgrade a start of the sandbox would have chosen.
    let authorization_id = decided.wake.authorization.unwrap().authorization_id;
    assert!(authorization_id.contains(E0));
    let recorded = (
        Some(decided.operation_id),
        Some(E1.to_string()),
        Some(authorization_id),
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);

    assert_not_available_yet(s.upgrade().await);

    s.assert_nothing_placed_or_sent("after the retry");
    let (retried_generation, target, live) = s.active_execution().await?.expect("the claim");
    assert_eq!(
        retried_generation, generation,
        "the retry claimed a new execution"
    );
    assert_eq!((target.as_deref(), live), (Some("stopped"), false));
    assert_eq!(s.recorded_upgrade().await?, recorded);
    assert_eq!(s.lifecycle().await?, "stopped");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_on_an_allowed_environment_is_left_alone() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("as recorded").await
}

#[tokio::test(flavor = "current_thread")]
async fn a_stranded_sandbox_is_refused_before_anything_is_placed() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = '{}' WHERE environment_ref = $1"
    ))
    .bind(E0)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("stranded").await
}

#[rstest::rstest]
#[case::started("started")]
#[case::suspended("suspended")]
#[case::paused("paused")]
#[tokio::test(flavor = "current_thread")]
async fn only_a_stopped_sandbox_is_upgraded_without_a_start(
    #[case] lifecycle: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(s.sandbox_id)
        .bind(lifecycle)
        .execute(&s.db.pool)
        .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Conflict { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_is_on_no_node_is_refused() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = NULL, target_node_id = NULL WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = NULL WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    // Evacuated: the disk is durable in object storage and on no node.
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        &s.db.pool,
        s.sandbox_id,
        pplx_space_common_core::types::LocalArtifactId::new(),
        "s3://space-test/evacuated-disk?region=us-east-1",
    )
    .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name,
        None
    );

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// An upgrade whose claim's lease still runs may own the execution, so
/// another is refused whatever it would decide now: a publication that makes
/// it "as recorded" doesn't let it retire that execution.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_in_flight_refuses_another() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;

    for publish in [false, true] {
        if publish {
            s.publish_upgraded().await?;
        }
        let error = s.upgrade().await.unwrap_err();

        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        assert!(s.pods.exists());
        assert_eq!(
            s.active_execution().await?,
            Some((7, Some("stopped".to_string()), true))
        );
    }
    Ok(())
}

/// An earlier attempt recorded an upgrade the node may have begun. The retry
/// keeps that upgrade although the policy no longer asks for it; it never
/// decides afresh over it.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_keeps_the_upgrade_an_earlier_attempt_recorded() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    assert_not_available_yet(s.upgrade().await);
    let recorded = s.recorded_upgrade().await?;
    {
        use pplx_space_runtime_environment_policy_store::{
            PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
        };
        let mut tx = s.db.pool.begin().await?;
        set_environment_state(
            &mut tx,
            E0.parse()?,
            RuntimeEnvironmentState::Allowed,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test allow")?,
            },
        )
        .await?;
        tx.commit().await?;
    }
    s.forget_cached_policy();

    assert_not_available_yet(s.upgrade().await);

    assert_eq!(s.recorded_upgrade().await?, recorded);
    s.assert_nothing_placed_or_sent("after the retry");
    Ok(())
}

/// An earlier attempt claimed the execution, placed its pod and recorded
/// nothing: the retry retires it, pod and all, then decides afresh and
/// records the upgrade on a new claim.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_that_recorded_nothing_is_retired_before_deciding() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;

    assert_not_available_yet(s.upgrade().await);

    assert!(!s.pods.exists(), "the earlier attempt's pod outlived it");
    assert_eq!(s.pods.deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    let (generation, target, live) = s.active_execution().await?.expect("the new claim");
    assert!(generation > 7, "{generation}");
    assert_eq!((target.as_deref(), live), (Some("stopped"), false));
    assert!(s.recorded_upgrade().await?.0.is_some());
    s.assert_nothing_placed_or_sent("after the retry");
    Ok(())
}

/// An earlier attempt claimed an execution and created its pod, then ended
/// before recording the pod's UID or anything else. The retry identifies the
/// pod by its name, which the generation fixes, and retires it.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_whose_pod_uid_went_unrecorded_is_identified_and_retired() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = NULL WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;

    assert_not_available_yet(s.upgrade().await);

    assert_eq!(s.pods.deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!s.pods.exists());
    Ok(())
}

/// Retiring an upgrade's execution that changed after it was read, so the
/// fenced update matches no row, isn't done: the execution stays active with
/// what it recorded, and the retire answers a retryable error rather than
/// letting the operation report success.
#[tokio::test(flavor = "current_thread")]
async fn a_retire_whose_fenced_update_misses_is_not_done() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let attempt = left_by(&s).await?;
    // Stands in for a change between the retire's read and its fenced
    // update: the update that would finish the execution matches nothing.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION keep_execution_active() RETURNS trigger AS $$
         BEGIN
             IF OLD.active AND NOT NEW.active THEN RETURN NULL; END IF;
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER keep_execution_active
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION keep_execution_active();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s
        .substrate
        .retire_upgrade_boot(s.sandbox_id, 7, attempt, false)
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(
        s.active_execution()
            .await?
            .is_some_and(|(generation, _, _)| generation == 7)
    );
    Ok(())
}

/// A re-send that fails to record the earlier attempt's upgrade again ends
/// its claim and keeps the execution, with what it recorded, for the next
/// retry.
#[tokio::test(flavor = "current_thread")]
async fn a_resend_failing_to_record_keeps_the_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    assert_not_available_yet(s.upgrade().await);
    let recorded = s.recorded_upgrade().await?;
    // The store refuses to record the upgrade again.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_runtime_upgrade_record() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'recording refused'; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER refuse_runtime_upgrade_record
             BEFORE UPDATE OF runtime_upgrade_authorization_id ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION refuse_runtime_upgrade_record();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(error.to_string().contains("record"), "{error:?}");
    assert_eq!(
        s.active_execution()
            .await?
            .map(|(_, target, live)| (target, live)),
        Some((Some("stopped".to_string()), false))
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    s.assert_nothing_placed_or_sent("after the failed record");
    Ok(())
}

/// Give the sandbox an active execution whose attempt targets `target` under
/// a lease ending `lease_secs` from now, with its pod in place: what a start
/// or an earlier upgrade leaves while it runs, or after it crashed.
async fn held_execution(s: &Subject, target: &str, lease_secs: i64) -> Result<(), BoxError> {
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    insert_k8s_execution(
        &s.db.pool,
        s.sandbox_id,
        7,
        &pod_name,
        &format!("{pod_name}-uid"),
        Some("127.0.0.1"),
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                route_activation_id = attempt.id,
                activation_attempt_target_state = $2,
                activation_attempt_expires_at = NOW() + make_interval(secs => $3::BIGINT),
                k8s_node_name = 'node-a'
           FROM (SELECT gen_random_uuid() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(target)
    .bind(lease_secs)
    .execute(&s.db.pool)
    .await?;
    *s.pods.pod.lock().unwrap() = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name, "namespace": "space", "uid": format!("{pod_name}-uid") },
        "status": { "phase": "Running", "podIP": "127.0.0.1" }
    }));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_in_flight_keeps_its_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", 300).await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    assert!(s.pods.exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), true))
    );
    Ok(())
}

/// A start that crashed leaves an expired claim targeting `started`, maybe
/// over a running guest. It isn't an upgrade's leftover, so a sandbox the
/// decision leaves alone keeps it for the start's own recovery.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_start_is_never_taken_for_an_upgrade_leftover() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", -3600).await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    assert!(s.pods.exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), false))
    );
    Ok(())
}

/// A drain is moving the stopped sandbox's disk off its node: a retryable
/// refusal until the disk is placed again, and nothing is placed meanwhile.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_moving_off_a_draining_node_is_refused_for_now() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// A start that moved the sandbox on owns the execution an earlier upgrade
/// left: the takeover, which checks the lifecycle under the sandbox's lock,
/// refuses it and leaves its attempt alone.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_sandbox_a_start_moved_on() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            left_by(&s).await?,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Conflict { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    Ok(())
}

/// An attempt that replaced the claim the retry read owns the execution: the
/// takeover, which checks that attempt under the sandbox's lock, refuses it and
/// leaves the replacement alone.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_claim_another_attempt_replaced() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let read = left_by(&s).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = gen_random_uuid()
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;
    let replacement = left_by(&s).await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            read,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Conflict { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(left_by(&s).await?, replacement);
    Ok(())
}

/// The attempt holding the claim a retry reads before its takeover.
async fn left_by(
    s: &Subject,
) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, BoxError> {
    Ok(s.substrate
        .store
        .stopped_runtime_upgrade_claim(s.sandbox_id)
        .await?
        .ok_or("no stopped runtime upgrade claim")?
        .activation_attempt_id)
}
