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
-- Migration: 002_user_password_column
-- Description: Adds an application-level password column to
--              sensorthings."User" to support Python-side (passlib/bcrypt)
--              credential management.
--
-- Background:
--   The original authentication design stored user credentials exclusively
--   inside PostgreSQL's internal pg_authid catalog (via CREATE USER ...
--   WITH ENCRYPTED PASSWORD).  This approach tied application users to
--   individual PostgreSQL login roles, which contradicts the intended
--   architecture: the FastAPI backend connects via a single master service
--   account, and istSOS users are strictly application-level entities.
--
--   This column stores a bcrypt hash (produced by passlib on the Python side)
--   and is nullable so that:
--     * Existing rows (admin user, OIDC users) are not broken.
--     * A NULL value is a safe sentinel: the password endpoint will return
--       HTTP 400 ("no local credential set") rather than crashing.
--     * OIDC users (auth_provider IS NOT NULL) will always have NULL here,
--       which is consistent and expected.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ----------------------------------------------------------------
        -- 1. Add the password column (bcrypt hash, nullable)
        -- ----------------------------------------------------------------
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "password" VARCHAR(255) DEFAULT NULL;

        -- ----------------------------------------------------------------
        -- 2. Add a comment describing the hashing scheme so future
        --    developers do not accidentally store plaintext values.
        -- ----------------------------------------------------------------
        COMMENT ON COLUMN sensorthings."User"."password" IS
            'bcrypt hash of the local istSOS credential, produced by '
            'passlib on the Python side. NULL for OIDC users and for '
            'accounts created before this migration was applied.';

        RESET ROLE;

    END IF;
END $BODY$;
