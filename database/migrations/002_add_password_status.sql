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
-- Migration: 002_add_password_status
-- Description: Adds application-layer credential columns to
--              sensorthings."User" to complete the transition away from
--              PostgreSQL pg_authid as the authentication source-of-truth.
--
-- password VARCHAR(255) DEFAULT NULL
--   Stores the bcrypt hash of the user's local password.  NULL signals that
--   the account has not yet been migrated and the legacy pg_authid fallback
--   should be used on the next login (JIT migration).  After a successful
--   fallback the hash is backfilled here so subsequent logins are bcrypt-only.
--
-- status  VARCHAR(50) DEFAULT 'active'
--   Account lifecycle flag.  Defaults to 'active' so all existing rows are
--   unaffected.  Future values: 'suspended', 'deleted'.  The application
--   layer enforces this; no database-level constraint is added here to keep
--   the migration simple and forward-compatible.
--
-- Both columns use ADD COLUMN IF NOT EXISTS so the migration is fully
-- idempotent and safe to re-run against an instance that already received
-- these columns via a manual hotfix.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ----------------------------------------------------------------
        -- 1. Application-layer bcrypt password hash
        --    NULL  → legacy account; triggers pg_authid JIT fallback
        --    NOT NULL → modern account; verified with passlib bcrypt
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "password" VARCHAR(255) DEFAULT NULL;

        -- ----------------------------------------------------------------
        -- 2. Account status / lifecycle flag
        --    'active'    → normal login permitted (default for all rows)
        --    'suspended' → login blocked by application layer
        --    'deleted'   → soft-delete; row retained for audit trail
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "status" VARCHAR(50) DEFAULT 'active';

        RESET ROLE;

    END IF;
END $BODY$;
