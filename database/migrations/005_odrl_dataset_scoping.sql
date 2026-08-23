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
-- Migration: 005_odrl_dataset_scoping
-- Description: First real (prototype-scale) implementation of dataset-scoped
--              row-level access, replacing the previous situation where
--              dataset_id/odrl_policy_id were captured at registration and
--              approval but only ever written to AuditLog -- never used to
--              build an actual access-control predicate.
--
-- Scope note
-- ----------
-- This is deliberately a minimal, honest prototype, not a full ODRL policy
-- engine:
--   * dataset_id is added ONLY to Datastream (the table the existing test
--     suite's dataset-scoping check already targets). A complete
--     implementation would need it (or a join path to it) on every governed
--     table; that is future work, not attempted here.
--   * odrl_policy_id itself is still NOT parsed. The dataset_id already
--     submitted through POST /Register and PATCH .../policy-approval is
--     used directly as the scoping value. There is no ODRL document
--     interpretation -- that would be the next real step once a real ODRL
--     policy source exists to parse.
--
-- Design decisions
-- ----------------
-- * dataset_id is nullable TEXT, default NULL, so existing rows are
--   unaffected -- matches the pattern used for is_public in
--   004_public_access.sql.
--
-- * odrl_governed_policy() mirrors viewer_policy()'s structure exactly
--   (see istsos_auth.sql) but takes a third dataset_id_ argument and
--   builds USING (dataset_id = <value>) instead of USING (TRUE). Scoped to
--   Datastream only, for the reason above.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ------------------------------------------------------------
        -- 1. Add the dataset_id column to Datastream.
        -- ------------------------------------------------------------
        ALTER TABLE sensorthings."Datastream"
            ADD COLUMN IF NOT EXISTS "dataset_id" TEXT DEFAULT NULL;

        -- ------------------------------------------------------------
        -- 2. odrl_governed_policy(users_, policyname_, dataset_id_)
        --    Grants the named users SELECT on Datastream rows matching
        --    exactly the given dataset_id -- the first real per-dataset
        --    predicate in this codebase, replacing USING (TRUE).
        -- ------------------------------------------------------------
        CREATE OR REPLACE FUNCTION sensorthings.odrl_governed_policy(
            users_ text[], policyname_ text, dataset_id_ text
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            user_list_ text;
        BEGIN
            SELECT string_agg(quote_ident(u), ', ') INTO user_list_ FROM unnest(users_) AS u;

            EXECUTE format(
                'CREATE POLICY %I
                ON sensorthings."Datastream"
                FOR SELECT
                TO %s
                USING (dataset_id = %L);',
                policyname_ || '_odrl_governed_datastream', user_list_, dataset_id_
            );
        END;
        $function$;

        RESET ROLE;

    END IF;
END $BODY$;
