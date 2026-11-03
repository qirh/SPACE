//! Postgres fixture for the admin store test suites.
//!
//! Thin shim over the workspace-shared `pplx-space-db-test-fixtures` crate:
//! [`postgres_fixture`] returns `Ok(None)` when no Docker daemon is reachable
//! so the suites skip silently (a green run that skipped these is not a green
//! run that exercised them — bring Docker up). The shared crate clones a
//! per-test database from a migrated template, so every `db/postgres`
//! migration (including the node-group tables) is already applied.

pub(super) use pplx_space_db_test_fixtures::TestDb as Fixture;

/// `Result` alias with a `()` default, matching the call sites' `-> TestResult`.
pub(super) type TestResult<T = ()> = pplx_space_db_test_fixtures::TestDbResult<T>;

/// Graceful fixture: `Ok(None)` when the Postgres testcontainer is
/// unavailable, otherwise a migrated per-test database. `Fixture.pool` is the
/// connection pool.
pub(super) async fn postgres_fixture() -> TestResult<Option<Fixture>> {
    pplx_space_db_test_fixtures::try_pg_test_db().await
}
