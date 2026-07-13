-- Copyright 2025 SUPSI
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- =============================================================================
-- Migration: 003_audit_log
-- Description: Creates the sensorthings."AuditLog" table to support the
--              STAC/ODRL access-governance audit trail.
--
-- Design decisions
-- ----------------
-- * UUID primary key (gen_random_uuid via pgcrypto, already loaded) avoids
--   sequential ID leakage and makes log rows safe to expose in external APIs.
--
-- * actor_id is a nullable FK to sensorthings."User"(id) with ON DELETE SET NULL
--   so that deleting a user never silently removes their audit trail.
--
-- * action_type is constrained to three well-known values:
--     'PUBLIC_READ'         - anonymous/unauthenticated STAC data access
--     'RESTRICTED_REQUEST'  - user submits an ODRL policy access request
--     'ADMIN_APPROVAL'      - administrator approves or denies a request
--
-- * The table is append-only by design: UPDATE and DELETE are revoked from
--   ALL roles so that no application code — even running as administrator —
--   can tamper with the audit trail.  Only INSERT is permitted.
--
-- * REVOKE and GRANT statements follow the istsos_auth.sql pattern: they run
--   AFTER RESET ROLE so the session superuser (not the administrator role)
--   issues the privilege changes.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ----------------------------------------------------------------
        -- 1. Create the AuditLog table
        -- ----------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS sensorthings."AuditLog" (
            "id"             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            "actor_id"       BIGINT       REFERENCES sensorthings."User"(id)
                                          ON DELETE SET NULL,
            "action_type"    VARCHAR(50)  NOT NULL
                                          CHECK ("action_type" IN (
                                              'PUBLIC_READ',
                                              'RESTRICTED_REQUEST',
                                              'ADMIN_APPROVAL'
                                          )),
            "dataset_id"     TEXT,
            "odrl_policy_id" TEXT,
            "payload"        JSONB        DEFAULT NULL,
            "created_at"     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );

        -- ----------------------------------------------------------------
        -- 2. Performance indexes
        --    action_type  — common filter: "show me all RESTRICTED_REQUESTs"
        --    actor_id     — common filter: "show me all actions by user X"
        -- ----------------------------------------------------------------
        CREATE INDEX IF NOT EXISTS "idx_auditlog_action_type"
            ON sensorthings."AuditLog" USING btree ("action_type" ASC NULLS LAST)
            TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS "idx_auditlog_actor_id"
            ON sensorthings."AuditLog" USING btree ("actor_id" ASC NULLS LAST)
            TABLESPACE pg_default;

        RESET ROLE;

        -- ----------------------------------------------------------------
        -- 3. Privilege setup — runs as session superuser (after RESET ROLE)
        --    following the istsos_auth.sql lines 398-444 pattern.
        --
        --    Append-only enforcement:
        --      - Revoke UPDATE and DELETE from every application role so the
        --        audit trail cannot be altered once written.
        --      - Grant INSERT to "user" and "sensor" so the application can
        --        record events without elevated privileges.
        --    The "administrator" role inherits ALL PRIVILEGES from the
        --    GRANT ALL ... TO "administrator" that ran at schema init time;
        --    we explicitly revoke UPDATE/DELETE from it too so even admin
        --    application code cannot mutate past log rows.
        -- ----------------------------------------------------------------
        REVOKE UPDATE, DELETE ON sensorthings."AuditLog" FROM "administrator";
        REVOKE UPDATE, DELETE ON sensorthings."AuditLog" FROM "user";
        REVOKE UPDATE, DELETE ON sensorthings."AuditLog" FROM "sensor";
        REVOKE UPDATE, DELETE ON sensorthings."AuditLog" FROM "guest";
        REVOKE UPDATE, DELETE ON sensorthings."AuditLog" FROM "qc";

        GRANT INSERT ON sensorthings."AuditLog" TO "user";
        GRANT INSERT ON sensorthings."AuditLog" TO "sensor";

    END IF;
END $BODY$;
