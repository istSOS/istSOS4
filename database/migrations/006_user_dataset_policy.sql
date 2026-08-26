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
-- Migration: 006_user_dataset_policy
-- Description: Persists the dataset/policy a user requested at registration
--              directly on sensorthings."User", instead of only ever writing
--              it into AuditLog. Needed for two reasons:
--
--   1. Admins reviewing the pending queue via GET /Users could not
--      previously see what a user actually asked for without manually
--      joining AuditLog -- the request was write-only metadata.
--
--   2. The OIDC/JIT-provisioning path (oidc_user_crud.py) had no
--      dataset_id/odrl_policy_id fields at all, so an externally-
--      authenticated user could never be bound to a dataset the way a
--      POST /Register user could. This column is what makes that possible,
--      and what api/app/v1/endpoints/create/activate_user.py now reads to
--      dispatch to odrl_governed_policy() the same way admin_approval.py
--      already does for the local registration path.
--
-- Design decisions
-- ----------------
-- * Nullable TEXT, default NULL -- same pattern as dataset_id on
--   Datastream (005_odrl_dataset_scoping.sql) and is_public
--   (004_public_access.sql). Existing rows are unaffected.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "dataset_id" TEXT DEFAULT NULL;

        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "odrl_policy_id" TEXT DEFAULT NULL;

        RESET ROLE;

    END IF;
END $BODY$;
