FROM timescale/timescaledb-ha:pg18-ts2.25
COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
COPY ./istsos_auth.sql /docker-entrypoint-initdb.d/12_init_auth.sql
COPY ./istsos_schema_versioning.sql /docker-entrypoint-initdb.d/13_init_db_istsos_schema_versioning.sql

# database/migrations/*.sql are applied on container init (they were
# previously only ever run by hand against a live dev database, so a fresh
# clone got a database missing the OIDC/password/audit/RBAC columns and
# policies). Each migration guards itself on custom.authorization and uses
# IF NOT EXISTS throughout, so this is safe and idempotent.
#
# Ordered by the numbering in database/migrations/.
COPY ./migrations/001_identity_linking.sql /docker-entrypoint-initdb.d/14_migration_001_identity_linking.sql
COPY ./migrations/002_add_password_status.sql /docker-entrypoint-initdb.d/15_migration_002_add_password_status.sql
COPY ./migrations/003_audit_log.sql /docker-entrypoint-initdb.d/16_migration_003_audit_log.sql
COPY ./migrations/004_admin_rejection.sql /docker-entrypoint-initdb.d/17_migration_004_admin_rejection.sql
COPY ./migrations/005_user_dataset_policy.sql /docker-entrypoint-initdb.d/18_migration_005_user_dataset_policy.sql
COPY ./migrations/006_session_scoped_rls_policies.sql /docker-entrypoint-initdb.d/19_migration_006_session_scoped_rls_policies.sql
COPY ./migrations/007_requested_role.sql /docker-entrypoint-initdb.d/20_migration_007_requested_role.sql
COPY ./migrations/008_oidc_duplicate_hint.sql /docker-entrypoint-initdb.d/21_migration_008_oidc_duplicate_hint.sql
COPY ./migrations/009_backfill_user_uri.sql /docker-entrypoint-initdb.d/22_migration_009_backfill_user_uri.sql
COPY ./migrations/010_unscoped_writes_without_network.sql /docker-entrypoint-initdb.d/23_migration_010_unscoped_writes_without_network.sql
COPY ./migrations/011_guest_reference_data_only.sql /docker-entrypoint-initdb.d/24_migration_011_guest_reference_data_only.sql
COPY ./migrations/012_custom_policies_require_custom_role.sql /docker-entrypoint-initdb.d/25_migration_012_custom_policies_require_custom_role.sql
COPY ./migrations/013_audit_user_created_role_changed.sql /docker-entrypoint-initdb.d/26_migration_013_audit_user_created_role_changed.sql
COPY ./migrations/014_history_tables_network_scoped.sql /docker-entrypoint-initdb.d/27_migration_014_history_tables_network_scoped.sql
COPY ./migrations/015_obs_manager_foi_datastream_location.sql /docker-entrypoint-initdb.d/28_migration_015_obs_manager_foi_datastream_location.sql
COPY ./migrations/016_dataset_id_scope_conditional_on_network.sql /docker-entrypoint-initdb.d/29_migration_016_dataset_id_scope_conditional_on_network.sql
