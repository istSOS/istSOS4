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
-- Migration: 009_oidc_duplicate_hint
-- Description: Adds an advisory-only column so an administrator reviewing
--              the pending queue (GET /Users) can see when a brand-new
--              OIDC signup's email matches an already-existing, unrelated
--              account -- without the system ever auto-linking the two.
--
--              This is deliberately NOT account linking. It never changes
--              login behaviour, never merges data, and is never read by
--              any authorization check. It is a plain FK the admin can see
--              alongside the "id" column already returned by GET /Users,
--              exactly the same way AuditLog.actor_id (003_audit_log.sql)
--              is a plain, advisory-shaped FK rather than an enforced
--              invariant.
--
--              ON DELETE SET NULL, same reasoning as AuditLog.actor_id:
--              deactivating/removing the matched account must not block
--              or cascade into this row -- see delete/user.py's own note
--              on why hard deletes are avoided here in the first place.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "possible_duplicate_of" BIGINT
                REFERENCES sensorthings."User"(id) ON DELETE SET NULL
                DEFAULT NULL;

        RESET ROLE;

    END IF;
END $BODY$;
