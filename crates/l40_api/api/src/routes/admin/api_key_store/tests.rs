use super::*;
use crate::auth::hash_token;
use pplx_space_common_core::types::{OrgId, UserId};

use crate::routes::admin::test_fixtures::{TestResult, postgres_fixture};

#[tokio::test(flavor = "current_thread")]
async fn create_list_and_org_scoped_delete_preserve_key_metadata() -> TestResult {
    let Some(db) = postgres_fixture().await? else {
        return Ok(());
    };
    let (org_id, user_id) = sqlx::query_as::<_, (OrgId, UserId)>(sql!(
        "SELECT o.id, u.id
           FROM organizations o
           JOIN users u ON u.org_id = o.id
          ORDER BY o.id, u.id
          LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let key_id = ApiKeyId::new();
    let raw_key = "store-test-key-without-a-vendor-prefix";

    let created = create(
        &db.pool,
        CreateApiKey {
            id: key_id,
            org_id,
            name: "store test",
            key_hash: &hash_token(raw_key),
            created_by: user_id,
            allowed_surfaces: &[ApiSurface::Ecw, ApiSurface::Native],
        },
    )
    .await?;
    assert_eq!(created.id, key_id);
    assert_eq!(created.created_by, Some(user_id));
    assert_eq!(
        created.allowed_surfaces,
        vec![ApiSurface::Ecw, ApiSurface::Native]
    );
    for surface in [ApiSurface::Ecw, ApiSurface::Native] {
        assert!(
            crate::auth::get_api_key_org(&db.pool, raw_key, surface)
                .await?
                .is_some(),
            "new key must authenticate on its {surface} surface"
        );
    }

    let legacy_key_id = ApiKeyId::new();
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, created_by)
         VALUES ($1, $2, 'legacy unassigned', $3, NULL)"
    ))
    .bind(legacy_key_id)
    .bind(org_id)
    .bind(hash_token("e2b_legacy-unassigned"))
    .execute(&db.pool)
    .await?;

    let listed = list_for_organization(&db.pool, org_id).await?;
    assert!(listed.iter().any(|key| key.id == key_id));
    assert!(
        listed
            .iter()
            .any(|key| key.id == legacy_key_id && key.created_by.is_none()),
        "legacy keys without a surviving creator remain visible"
    );

    assert!(
        !delete(&db.pool, OrgId::new(), key_id).await?,
        "an organization cannot revoke another organization's key"
    );
    assert!(delete(&db.pool, org_id, key_id).await?);
    assert!(!delete(&db.pool, org_id, key_id).await?);
    Ok(())
}
