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
-- Migration: 013_audit_user_created_role_changed
-- Description: POST /Users (an administrator creating an account directly)
--              and PATCH /Users/{id}/role (changing a user's role and/or
--              Network scope) wrote nothing to the AuditLog, so both left no
--              trace. The API now records them as USER_CREATED and
--              ROLE_CHANGED; this extends the action_type CHECK constraint
--              to allow the two new values.
--
--              Postgres has no ALTER CHECK, so the constraint is dropped and
--              re-added, as 004_admin_rejection.sql does. Idempotent.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        ALTER TABLE sensorthings."AuditLog"
            DROP CONSTRAINT IF EXISTS "AuditLog_action_type_check";

        ALTER TABLE sensorthings."AuditLog"
            ADD CONSTRAINT "AuditLog_action_type_check"
            CHECK ("action_type" IN (
                'PUBLIC_READ',
                'RESTRICTED_REQUEST',
                'ADMIN_APPROVAL',
                'ADMIN_REJECTION',
                'USER_CREATED',
                'ROLE_CHANGED'
            ));

        RESET ROLE;

    END IF;
END $BODY$;
