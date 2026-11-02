use super::*;
use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

/// `GET /admin/api/v1/debug/sandbox/{id}` takes the lock wait as a retryable
/// 503 and never takes the write conflict.
///
/// This is the surface an operator reaches for while a sandbox is wedged, so
/// answering a lock wait with "Internal server error" is worst exactly when it
/// matters: the point of the read is to find out what is stuck. Two statements
/// could produce it — the authz preflight on `clusters` and the lifecycle read
/// that gates the whole response — and both went through arms that discarded
/// the error entirely (`Err(_) =>`) or logged it and returned 500.
///
/// Only what this wrapper adds is asserted: that it pins the scope to
/// `ReadOnly` — so a serialization abort takes the generic retryable 503, not
/// the write-specific 409 — and shapes a `Response`. The SQLSTATE table lives
/// once in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`,
/// and the handler arms that call this wrapper are covered by
/// `the_debug_handler_answers_a_contended_read_with_a_503` below — this one
/// alone would stay green if either arm dropped the call.
#[tokio::test]
async fn a_sandbox_debug_read_pins_the_scope_to_read_only()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    let lock_wait = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await;
    assert_eq!(
        contention_response(&lock_wait).map(|response| response.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "a lock wait on the debug read is retryable, not internal"
    );

    let deadlock = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await;
    assert_eq!(
        contention_response(&deadlock).map(|response| response.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "a debug read never takes the write-specific 409, but the retry succeeds"
    );

    // Outside the contention set: this one really is a 500.
    let violation = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert!(contention_response(&violation).is_none());

    // Closed like the sibling `raise_sqlstate` tests: each injection fails a
    // statement on the connection it borrows.
    db.pool.close().await;
    Ok(())
}

/// Both of the handler's database arms are actually wired to the disposition.
///
/// The test above asserts `contention_response` in isolation, which survives a
/// handler arm being changed or dropped — an independent review proved exactly
/// that by reverting both arms and watching it stay green. So this one drives
/// `get_sandbox_debug` itself, once per arm, against a real held lock:
/// `clusters` for the authz preflight and `sandboxes` for the lifecycle read
/// that gates the whole response. Same technique and same reason as
/// `controllers::sandbox::lifecycle::tests::a_contended_delete_precheck_stays_retryable`.
#[tokio::test]
async fn the_debug_handler_answers_a_contended_read_with_a_503()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use pplx_space_db_test_fixtures::SEED_ORG_ID;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // The role-level bound, so a blocked read aborts instead of waiting for the
    // lock to be released.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;
    let state = crate::routes::AppState::from_pool(bounded)
        .await
        .expect("app state from the test pool");

    // An org_admin of the managing org, so the gate reaches its `clusters`
    // read rather than short-circuiting on the role check.
    let actor = User {
        id: UserId::new(),
        email: "contention-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "contention probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    // `ACCESS EXCLUSIVE` is what DDL takes; it conflicts with each read's
    // `ACCESS SHARE`, so the read blocks and then aborts with `55P03`.
    for table in ["clusters", "sandboxes"] {
        let mut blocker = db.pool.begin().await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
        )))
        .execute(&mut *blocker)
        .await?;

        let response = get_sandbox_debug(
            State(state.clone()),
            axum::Extension(crate::middleware::auth::AuthUser(actor.clone())),
            Path(SandboxId::new()),
        )
        .await;

        blocker.rollback().await?;

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a debug read blocked behind DDL on {table} is retryable, not internal"
        );
    }

    db.pool.close().await;
    Ok(())
}

/// The debug read reports which environment variables a sandbox has without
/// their values, in the `{ "NAME": null }` shape that replaced
/// `{ "NAME": "value" }`.
#[tokio::test]
async fn the_debug_response_lists_env_names_without_values()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use axum::extract::{Path, State};
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let state = crate::routes::AppState::from_pool(db.pool.clone())
        .await
        .expect("app state from the test pool");
    let sandbox_id = SandboxId::new();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation, env
         )
         VALUES ($1, $2, $3, 'started', 'unplaced', 'k8s', 1, $4)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(sqlx::types::Json(serde_json::json!({
        "SYNTHETIC_TOKEN": "synthetic-value-1",
        "EMPTY": "",
    })))
    .execute(&db.pool)
    .await?;
    let actor = User {
        id: UserId::new(),
        email: "debug-reader@example.test".to_string(),
        password_hash: String::new(),
        display_name: "debug reader".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    let response = get_sandbox_debug(
        State(state),
        axum::Extension(crate::middleware::auth::AuthUser(actor)),
        Path(sandbox_id),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1_000_000).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body["resources"]["env"],
        serde_json::json!({ "SYNTHETIC_TOKEN": null, "EMPTY": null })
    );
    assert!(!body.to_string().contains("synthetic-value-1"));
    db.pool.close().await;
    Ok(())
}
