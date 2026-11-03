use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::{ApiKeyId, OrgId, UserId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{
    api_key_store::{self, CreateApiKey},
    support::{self, ContentionScope, api_error},
};
use crate::{
    api_surface::ApiSurface, auth::hash_token, middleware::auth::AuthUser, routes::AppState,
};

const API_KEY_NAME_MAX_CHARS: usize = 128;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateApiKeyRequest {
    name: String,
    allowed_surfaces: Vec<ApiSurface>,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiKeyMetadata {
    id: ApiKeyId,
    org_id: OrgId,
    name: String,
    created_by: Option<UserId>,
    created_by_email: Option<String>,
    allowed_surfaces: Vec<ApiSurface>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

#[derive(Serialize)]
pub(super) struct CreatedApiKey {
    api_key: ApiKeyMetadata,
    secret: String,
}

impl From<api_key_store::ApiKeyRow> for ApiKeyMetadata {
    fn from(row: api_key_store::ApiKeyRow) -> Self {
        Self {
            id: row.id,
            org_id: row.org_id,
            name: row.name,
            created_by: row.created_by,
            created_by_email: row.created_by_email,
            allowed_surfaces: row.allowed_surfaces,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

pub(super) async fn list_api_keys(
    State(state): State<AppState>,
    Path(org_id): Path<OrgId>,
) -> Response {
    match api_key_store::organization_exists(&state.pool, org_id).await {
        Ok(false) => {
            return api_error(StatusCode::NOT_FOUND, "Organization not found").into_response();
        }
        Ok(true) => {}
        Err(error) => return read_error(error),
    }
    match api_key_store::list_for_organization(&state.pool, org_id).await {
        Ok(keys) => Json(
            keys.into_iter()
                .map(ApiKeyMetadata::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => read_error(error),
    }
}

pub(super) async fn create_api_key(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Response {
    if org_id != actor.org_id {
        return api_error(
            StatusCode::FORBIDDEN,
            "API keys can only be created for your organization",
        )
        .into_response();
    }
    let name = match api_key_name(&request.name) {
        Ok(name) => name,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message).into_response(),
    };
    let allowed_surfaces = match allowed_surfaces(request.allowed_surfaces) {
        Ok(surfaces) => surfaces,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message).into_response(),
    };
    let secret = generate_api_key();
    let key_hash = hash_token(&secret);
    let input = CreateApiKey {
        id: ApiKeyId::new(),
        org_id,
        name,
        key_hash: &key_hash,
        created_by: actor.id,
        allowed_surfaces: &allowed_surfaces,
    };
    match api_key_store::create(&state.pool, input).await {
        Ok(row) => (
            StatusCode::CREATED,
            [(header::CACHE_CONTROL, "no-store")],
            Json(CreatedApiKey {
                api_key: row.into(),
                secret,
            }),
        )
            .into_response(),
        Err(error) => support::db_error(error).into_response(),
    }
}

pub(super) async fn delete_api_key(
    State(state): State<AppState>,
    Path((org_id, api_key_id)): Path<(OrgId, ApiKeyId)>,
) -> Response {
    match api_key_store::delete(&state.pool, org_id, api_key_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => api_error(StatusCode::NOT_FOUND, "API key not found").into_response(),
        Err(error) => support::db_error(error).into_response(),
    }
}

fn api_key_name(raw: &str) -> Result<&str, &'static str> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("API key name must not be empty");
    }
    if name.chars().count() > API_KEY_NAME_MAX_CHARS {
        return Err("API key name must be at most 128 characters");
    }
    if name.chars().any(char::is_control) {
        return Err("API key name must not contain control characters");
    }
    Ok(name)
}

fn allowed_surfaces(surfaces: Vec<ApiSurface>) -> Result<Vec<ApiSurface>, &'static str> {
    let mut ecw = false;
    let mut native = false;
    for surface in surfaces {
        let seen = match surface {
            ApiSurface::Ecw => &mut ecw,
            ApiSurface::Native => &mut native,
        };
        if *seen {
            return Err("Allowed APIs must not contain duplicates");
        }
        *seen = true;
    }
    match (ecw, native) {
        (false, false) => Err("Select at least one allowed API"),
        (true, false) => Ok(vec![ApiSurface::Ecw]),
        (false, true) => Ok(vec![ApiSurface::Native]),
        (true, true) => Ok(vec![ApiSurface::Ecw, ApiSurface::Native]),
    }
}

fn generate_api_key() -> String {
    let mut bytes = [0_u8; 32];
    rand::fill(&mut bytes);
    hex::encode(bytes)
}

fn read_error(error: sqlx::Error) -> Response {
    if let Some((status, message)) =
        support::contention_disposition(ContentionScope::ReadOnly, &error)
    {
        return api_error(status, message).into_response();
    }
    tracing::warn!(%error, "admin API key read failed");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

#[cfg(test)]
mod tests;
