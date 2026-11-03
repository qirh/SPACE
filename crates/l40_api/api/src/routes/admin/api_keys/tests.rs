use super::*;
use crate::models::User;
use pplx_space_common_core::{sql, types::OrgId};

#[test]
fn api_key_request_validation_normalizes_names_and_surfaces() {
    assert_eq!(api_key_name("  deployment key  "), Ok("deployment key"));
    api_key_name("\n").unwrap_err();
    api_key_name(&"x".repeat(API_KEY_NAME_MAX_CHARS + 1)).unwrap_err();

    assert_eq!(
        allowed_surfaces(vec![ApiSurface::Native, ApiSurface::Ecw]),
        Ok(vec![ApiSurface::Ecw, ApiSurface::Native])
    );
    allowed_surfaces(Vec::new()).unwrap_err();
    allowed_surfaces(vec![ApiSurface::Ecw, ApiSurface::Ecw]).unwrap_err();
}

#[test]
fn generated_api_keys_are_opaque_256_bit_secrets() {
    let key = generate_api_key();
    assert_eq!(key.len(), 64);
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test(flavor = "current_thread")]
async fn creation_is_bound_to_the_calling_admins_organization_and_identity()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = sqlx::query_as::<_, User>(sql!(
        "SELECT u.*
           FROM users u
           JOIN clusters c ON c.managing_org_id = u.org_id
          WHERE c.singleton
            AND u.role = 'organization_admin'
          ORDER BY u.id
          LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let other_org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Other API key org', $2)"
    ))
    .bind(other_org_id)
    .bind(format!("other-api-key-org-{other_org_id}"))
    .execute(&db.pool)
    .await?;

    let cross_org_response = create_api_key(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(other_org_id),
        Json(CreateApiKeyRequest {
            name: "cross-org key".to_string(),
            allowed_surfaces: vec![ApiSurface::Ecw],
        }),
    )
    .await;
    assert_eq!(cross_org_response.status(), StatusCode::FORBIDDEN);

    let own_org_response = create_api_key(
        State(state),
        Extension(AuthUser(actor.clone())),
        Path(actor.org_id),
        Json(CreateApiKeyRequest {
            name: "creator-bound key".to_string(),
            allowed_surfaces: vec![ApiSurface::Native],
        }),
    )
    .await;
    assert_eq!(own_org_response.status(), StatusCode::CREATED);

    let created_by = sqlx::query_scalar::<_, UserId>(sql!(
        "SELECT created_by
           FROM api_keys
          WHERE org_id = $1 AND name = 'creator-bound key'"
    ))
    .bind(actor.org_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(created_by, actor.id);
    Ok(())
}
