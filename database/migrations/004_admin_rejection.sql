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
-- Migration: 004_admin_rejection
-- Description: Adds 'ADMIN_REJECTION' to the sensorthings."AuditLog"
--              action_type CHECK constraint, to support the cyclical
--              approval workflow (pending -> rejected -> re-registration).
--
-- Design decisions
-- ----------------
-- * Postgres has no ALTER CHECK; the existing constraint is dropped and
--   recreated with the additional value.  The constraint name matches the
--   auto-generated name from 003_audit_log.sql ("AuditLog_action_type_check").
--
-- * No change to the User.status column: it is an unconstrained VARCHAR(50)
--   (default 'active'), so 'rejected' requires no schema change there.
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
                'ADMIN_REJECTION'
            ));

        RESET ROLE;

    END IF;
END $BODY$;
