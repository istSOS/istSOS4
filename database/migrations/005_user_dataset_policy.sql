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
-- Migration: 005_user_dataset_policy
-- Description: Adds sensorthings."User".dataset_id -- the name of the Network
--              a user is scoped to. Set at approval / activation from the
--              applicant's request; read back by the RLS predicates in
--              006_session_scoped_rls_policies.sql to filter Datastream and
--              Observation rows. NULL means no restriction.
--
--              Nullable TEXT, default NULL -- existing rows are unaffected.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- The Network the user is scoped to, by name. NULL = no
        -- restriction. Set at approval / activation from the applicant's
        -- request; read back by the RLS predicates in
        -- 006_session_scoped_rls_policies.sql.
        ALTER TABLE sensorthings."User"
            ADD COLUMN IF NOT EXISTS "dataset_id" TEXT DEFAULT NULL;

        RESET ROLE;

    END IF;
END $BODY$;
