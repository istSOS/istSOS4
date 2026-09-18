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
-- Migration: 001_identity_linking
-- Description: Adds OIDC identity-linking columns to sensorthings."User" table
--              to support Just-In-Time (JIT) provisioning from external auth
--              providers (e.g., an OIDC gateway connector).
--
-- These columns are nullable so that existing local (password-based) users are
-- completely unaffected. The partial unique index enforces that no two OIDC
-- accounts from the same provider share the same external subject identifier,
-- while still allowing multiple rows with NULL auth_provider (local users).
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ----------------------------------------------------------------
        -- 1. Add auth_provider column (NULL for local users)
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "auth_provider" VARCHAR(255) DEFAULT NULL;

        -- ----------------------------------------------------------------
        -- 2. Add external_sub_id column (the 'sub' claim from the OIDC JWT)
        --    NULL for local users.
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "external_sub_id" VARCHAR(512) DEFAULT NULL;

        -- ----------------------------------------------------------------
        -- 3. Partial unique index: guarantees no duplicate (provider, sub)
        --    pair for OIDC-linked accounts. Local accounts (auth_provider IS
        --    NULL) are excluded from this uniqueness constraint by design.
        -- ----------------------------------------------------------------
        CREATE UNIQUE INDEX IF NOT EXISTS "uq_user_auth_provider_sub_id"
            ON sensorthings."User" ("auth_provider", "external_sub_id")
            WHERE "auth_provider" IS NOT NULL;

        RESET ROLE;

    END IF;
END $BODY$;
