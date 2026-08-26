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
-- Migration: 004_public_access
-- Description: Implements Path A (Public Data Access) of the ODRL dual-path
--              architecture.
--
-- Design decisions
-- ----------------
-- * Adds an ``is_public`` BOOLEAN column (DEFAULT false, NOT NULL) to the
--   Datastream table.  Administrators can flag individual datastreams as
--   publicly accessible; all others remain restricted.
--
-- * Replaces the previous blanket anonymous_datastream / anonymous_observation
--   RLS policies (which allowed guests to SELECT all rows) with fine-grained
--   policies that enforce the is_public flag at the PostgreSQL kernel level.
--
-- * The Observation policy uses a correlated sub-query on Datastream.is_public
--   so that observation visibility is always derived from its parent datastream
--   — there is no separate per-observation flag to get out of sync.
--
-- * The AuditLog INSERT privilege for the 'guest' role is also granted here
--   so that public-read audit events can be written while the session runs
--   as the guest PostgreSQL role.
--
-- Idempotency
-- -----------
-- * ADD COLUMN is guarded with an IF NOT EXISTS check so re-running this
--   migration on an already-migrated database is safe.
-- * DROP POLICY IF EXISTS is used so the migration is safe even if the old
--   policies were never created (e.g. fresh installs with AUTHORIZATION=0).
-- * CREATE POLICY is NOT repeated — new policies are only created once per
--   database lifetime; the DROP + CREATE pair achieves idempotency.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ----------------------------------------------------------------
        -- 1. Add is_public column to Datastream
        --    DEFAULT false: existing datastreams are private until explicitly
        --    flagged by an administrator.
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."Datastream"
            ADD COLUMN IF NOT EXISTS is_public BOOLEAN NOT NULL DEFAULT false;

        -- ----------------------------------------------------------------
        -- 2. Drop the blanket anonymous SELECT policies that granted guests
        --    unrestricted read access to all rows.
        -- ----------------------------------------------------------------
        DROP POLICY IF EXISTS anonymous_datastream ON sensorthings."Datastream";
        DROP POLICY IF EXISTS anonymous_observation ON sensorthings."Observation";

        -- ----------------------------------------------------------------
        -- 3. Create fine-grained RLS policies for the guest role.
        --
        --    Datastream: visible to guest only when is_public = true.
        --    Observation: visible to guest only when its parent Datastream
        --                 is public (correlated sub-query keeps the two
        --                 tables in sync automatically).
        -- ----------------------------------------------------------------
        CREATE POLICY anonymous_datastream
            ON sensorthings."Datastream"
            FOR SELECT
            TO "guest"
            USING (is_public = true);

        CREATE POLICY anonymous_observation
            ON sensorthings."Observation"
            FOR SELECT
            TO "guest"
            USING (
                datastream_id IN (
                    SELECT id
                    FROM sensorthings."Datastream"
                    WHERE is_public = true
                )
            );

        RESET ROLE;

        -- ----------------------------------------------------------------
        -- 4. Grant INSERT on AuditLog to the guest role so that anonymous
        --    PUBLIC_READ audit events can be recorded while the session
        --    runs as the guest PostgreSQL role.
        --    The REVOKE of UPDATE / DELETE was already applied in migration
        --    003_audit_log.sql; we only need the INSERT grant here.
        -- ----------------------------------------------------------------
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'guest') THEN
                GRANT INSERT ON sensorthings."AuditLog" TO "guest";
            END IF;
        END $$;

    END IF;
END $BODY$;
