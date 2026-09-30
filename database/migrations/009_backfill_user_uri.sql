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
-- Migration: 009_backfill_user_uri
-- Description: Fills in sensorthings."User".uri for any row that has none.
--
--              JIT-provisioned OIDC accounts (app/db/oidc_user_crud.py,
--              create_pending_oidc_user) were inserted without the uri
--              backfill that create/register_request.py has always done,
--              so every externally-authenticated user had uri = NULL.
--
--              That was not cosmetic. set_commit()
--              (api/app/v1/endpoints/update/functions.py) writes
--                  Commit.author = current_user["uri"]
--              and sensorthings."Commit".author is NOT NULL -- so ANY write
--              by an OIDC user (editor / sensor / obs_manager / qc) aborted
--              on that constraint. The API reported it through
--              handle_integrity_violation() as
--                  400 "Invalid entity: a required value is missing or not
--                       allowed."
--              which reads like a bad request body rather than the real
--              cause, making it easy to misdiagnose.
--
--              The code path is fixed in oidc_user_crud.py; this migration
--              repairs rows already written by the previous behaviour.
--
--              Idempotent: only touches rows WHERE uri IS NULL, so re-running
--              it is a no-op and it never overwrites an existing uri.
--
--              Matches the format register_request.py writes ('/Users(<id>)').
--              Note create/user.py stores an absolute URL instead; that
--              inconsistency predates this migration and is left alone --
--              both forms are non-NULL, which is all Commit.author needs.
-- =============================================================================

DO $$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        UPDATE sensorthings."User"
        SET uri = '/Users(' || id || ')'
        WHERE uri IS NULL;

        RAISE NOTICE 'Migration 009: backfilled uri for % user row(s).',
            (SELECT count(*) FROM sensorthings."User" WHERE uri IS NOT NULL);

    END IF;
END
$$;
