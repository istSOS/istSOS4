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
-- Migration: 008_requested_role
-- Description: Lets an applicant state which RBAC role they want at
--              registration, instead of an administrator inventing one from
--              nothing at approval time.
--
--              Before this, RestrictedRegistrationRequest and the OIDC
--              provisioning path collected dataset_id/odrl_policy_id but no
--              role signal at all -- an admin reviewing GET /Users had
--              nothing structured to go on beyond a free-text explanation
--              field, and had to guess or ask separately what access level
--              made sense.
--
-- Design decisions
-- ----------------
-- * Nullable TEXT, same pattern as dataset_id/odrl_policy_id
--   (006_user_dataset_policy.sql). NULL for already-active users and for
--   accounts created directly via POST /Users, where an administrator
--   picks the role at creation time and there is no separate "request"
--   phase to record.
-- * Deliberately NOT constrained to VALID_RBAC_ROLES at the DB level --
--   validate_rbac_role() already enforces that at the API boundary
--   (RestrictedRegistrationRequest, the /auth/{provider}/login query
--   param), and a DB-level CHECK would have to be kept in lockstep with
--   that Python set by hand. Same tradeoff already made for role itself.
-- * The admin approval endpoints (admin_approval.py, activate_user.py)
--   treat this column as a *default*, not a mandate: if the caller's own
--   request supplies a role, that value wins; this is only read when the
--   caller omits one. The administrator stays the final gatekeeper.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "requested_role" TEXT DEFAULT NULL;

        RESET ROLE;

    END IF;
END $BODY$;
