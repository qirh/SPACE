//! Postgres operations for organization-scoped API keys.

use pplx_space_common_core::{
    sql,
    types::{ApiKeyId, OrgId, UserId},
};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;

use crate::api_surface::ApiSurface;

#[derive(Clone, Debug, FromRow)]
pub(super) struct ApiKeyRow {
    pub(super) id: ApiKeyId,
    pub(super) org_id: OrgId,
    pub(super) name: String,
    pub(super) created_by: Option<UserId>,
    pub(super) created_by_email: Option<String>,
    pub(super) allowed_surfaces: Vec<ApiSurface>,
    pub(super) created_at: OffsetDateTime,
    pub(super) updated_at: OffsetDateTime,
}

pub(super) async fn organization_exists(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM organizations WHERE id = $1)"
    ))
    .bind(org_id)
    .fetch_one(executor)
    .await
}

pub(super) async fn list_for_organization(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
) -> Result<Vec<ApiKeyRow>, sqlx::Error> {
    sqlx::query_as::<_, ApiKeyRow>(sql!(
        "SELECT k.id,
                k.org_id,
                k.name,
                k.created_by,
                u.email AS created_by_email,
                k.allowed_surfaces,
                k.created_at,
                k.updated_at
           FROM api_keys k
           LEFT JOIN users u ON u.id = k.created_by AND u.org_id = k.org_id
          WHERE k.org_id = $1
          ORDER BY k.created_at DESC, k.id DESC"
    ))
    .bind(org_id)
    .fetch_all(executor)
    .await
}

pub(super) struct CreateApiKey<'a> {
    pub(super) id: ApiKeyId,
    pub(super) org_id: OrgId,
    pub(super) name: &'a str,
    pub(super) key_hash: &'a str,
    pub(super) created_by: UserId,
    pub(super) allowed_surfaces: &'a [ApiSurface],
}

pub(super) async fn create(
    executor: impl PgExecutor<'_>,
    input: CreateApiKey<'_>,
) -> Result<ApiKeyRow, sqlx::Error> {
    // Return the creator label in the insert statement so a successful write
    // cannot be followed by a failed reload that loses the one-time secret
    // response. The CTE makes the inserted row available to that joined
    // projection without opening a second database round trip.
    sqlx::query_as::<_, ApiKeyRow>(sql!(
        "WITH inserted AS (
             INSERT INTO api_keys (
                 id, org_id, name, key_hash, created_by, allowed_surfaces,
                 federated_owner_assertions_enabled
             )
             VALUES ($1, $2, $3, $4, $5, $6, false)
             RETURNING id, org_id, name, created_by, allowed_surfaces,
                       created_at, updated_at
         )
         SELECT k.id,
                k.org_id,
                k.name,
                k.created_by,
                u.email AS created_by_email,
                k.allowed_surfaces,
                k.created_at,
                k.updated_at
           FROM inserted k
           LEFT JOIN users u ON u.id = k.created_by AND u.org_id = k.org_id"
    ))
    .bind(input.id)
    .bind(input.org_id)
    .bind(input.name)
    .bind(input.key_hash)
    .bind(input.created_by)
    .bind(input.allowed_surfaces)
    .fetch_one(executor)
    .await
}

pub(super) async fn delete(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    api_key_id: ApiKeyId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!("DELETE FROM api_keys WHERE id = $1 AND org_id = $2"))
        .bind(api_key_id)
        .bind(org_id)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests;
