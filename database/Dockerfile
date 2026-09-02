FROM timescale/timescaledb-ha:pg18-ts2.25
COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
COPY ./istsos_auth.sql /docker-entrypoint-initdb.d/12_init_auth.sql
COPY ./istsos_schema_versioning.sql /docker-entrypoint-initdb.d/13_init_db_istsos_schema_versioning.sql

# database/migrations/*.sql were previously never applied on container init --
# only ever run by hand against a live dev database. A fresh clone (a
# mentor's machine, CI, anyone else) got a database missing every one of
# these: OIDC identity-linking columns, the password/status columns, the
# whole AuditLog table, admin rejection, public access, ODRL dataset
# scoping, the session-scoped RLS policies, dataset_id/odrl_policy_id/
# requested_role on User, and the OIDC duplicate-hint column -- silently,
# since docker-entrypoint-initdb.d only runs once, on an empty data
# directory, so the gap wouldn't even resurface as an error until the API
# hit a missing column at request time.
#
# Each migration already guards itself on custom.authorization (set via
# the `-c custom.authorization=...` Postgres startup flag in
# docker-compose.yml/dev_docker-compose.yml, the same flag 12_init_auth.sql
# above already depends on) and uses IF NOT EXISTS / ADD COLUMN IF NOT
# EXISTS throughout, so copying them in here is safe and idempotent --
# the same reasoning that already applies to the three files above.
#
# Ordered by the numbering in database/migrations/, except the two
# 004-numbered files (independent of each other, confirmed no
# cross-references either direction): 004_public_access.sql predates
# 004_admin_rejection.sql chronologically, so it runs first.
COPY ./migrations/001_identity_linking.sql /docker-entrypoint-initdb.d/14_migration_001_identity_linking.sql
COPY ./migrations/002_add_password_status.sql /docker-entrypoint-initdb.d/15_migration_002_add_password_status.sql
COPY ./migrations/003_audit_log.sql /docker-entrypoint-initdb.d/16_migration_003_audit_log.sql
COPY ./migrations/004_public_access.sql /docker-entrypoint-initdb.d/17_migration_004_public_access.sql
COPY ./migrations/004_admin_rejection.sql /docker-entrypoint-initdb.d/18_migration_004_admin_rejection.sql
COPY ./migrations/005_odrl_dataset_scoping.sql /docker-entrypoint-initdb.d/19_migration_005_odrl_dataset_scoping.sql
COPY ./migrations/006_user_dataset_policy.sql /docker-entrypoint-initdb.d/20_migration_006_user_dataset_policy.sql
COPY ./migrations/007_session_scoped_rls_policies.sql /docker-entrypoint-initdb.d/21_migration_007_session_scoped_rls_policies.sql
COPY ./migrations/008_requested_role.sql /docker-entrypoint-initdb.d/22_migration_008_requested_role.sql
COPY ./migrations/009_oidc_duplicate_hint.sql /docker-entrypoint-initdb.d/23_migration_009_oidc_duplicate_hint.sql
COPY ./migrations/010_observation_dataset_scoping.sql /docker-entrypoint-initdb.d/24_migration_010_observation_dataset_scoping.sql
