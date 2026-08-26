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
-- Migration: 007_session_scoped_rls_policies
--
-- The bug this fixes
-- -------------------
-- Every RLS policy created by viewer_policy(), editor_policy(),
-- obs_manager_policy(), sensor_policy(), and qc_policy() was scoped
-- ``TO <username>`` -- a specific individual PostgreSQL role name. But
-- app/v1/endpoints/functions.py::set_role() only ever runs
-- ``SET LOCAL ROLE <group>`` (one of "user"/"sensor"/"qc"), never an
-- individual's own name. No application code path creates an individual
-- login role for a real user either -- create/user.py and
-- create/register_request.py both only ever INSERT into "User"; every
-- connection authenticates as the shared pool credentials and switches
-- role per-request. So these policies could never match any real session,
-- for any user, ever -- confirmed by querying pg_policy directly and
-- finding real per-username policies (from a test-fixture role, not a
-- genuine user) that still never fire.
--
-- The fix
-- -------
-- Move from per-user policies created at approval time to static, shared
-- policies scoped to the actual PostgreSQL group role sessions run as,
-- with the *individual* differentiation done by a small session variable
-- instead of a role name -- the same pattern Supabase uses (inject
-- auth.uid(), let policies join out from there). set_role() (see the
-- companion Python change) now also runs
-- ``SELECT set_config('app.current_user_id', $1, true)`` right after
-- switching role, and these policies read it back via three small helper
-- functions below.
--
-- One real subtlety this uncovered: DB_ROLE_BY_RBAC_ROLE maps BOTH
-- 'viewer' and 'editor' to the same Postgres group "user", and BOTH
-- 'sensor' and 'obs_manager' to the same group "sensor". A policy scoped
-- only ``TO "user"`` can no longer tell a viewer from an editor by role
-- name alone -- that distinction now has to be made *inside* the USING
-- clause, by checking the caller's actual application role via
-- current_app_user_role(). This is why editor/obs_manager get an
-- additional role-gated write policy layered on top of a shared
-- group-wide read policy, rather than one policy each.
--
-- Dataset scoping
-- ---------------
-- Per the agreed scope: dataset-scoping applies to viewer, editor,
-- obs_manager, sensor, and qc -- everything EXCEPT odrl_governed, which
-- is deliberately left untouched here for future ODRL work.
-- dataset_id only exists as a column on Datastream (see
-- 005_odrl_dataset_scoping.sql's own scope note), so only the
-- Datastream-level policies below are actually dataset-filtered; every
-- other table keeps a blanket USING (TRUE) for these roles, same
-- limitation odrl_governed already documents for itself. Extending
-- dataset_id to other tables is future work, not attempted here.
--
-- What this replaces
-- -------------------
-- viewer_policy(), editor_policy(), obs_manager_policy(), sensor_policy(),
-- qc_policy() are dropped entirely -- there is nothing left to call them
-- for. Policies are created ONCE, by this migration, not per-approval.
-- admin_approval.py, activate_user.py, and create/user.py all drop their
-- POLICY_FN_MAP dispatch for these five roles; approving/activating a
-- user into one of them is now a plain UPDATE, nothing else.
-- odrl_governed_policy() is NOT touched -- still called directly, still
-- per-approval, exactly as before.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- --------------------------------------------------------------
        -- 1. Drop the only per-username policies that ever existed --
        --    all nine belong to a test-fixture role ("pwtest_user"),
        --    never a genuine application user, and none of them have
        --    ever matched a real session either way.
        -- --------------------------------------------------------------
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN
                SELECT polname, relname
                FROM pg_policy p JOIN pg_class c ON p.polrelid = c.oid
                WHERE polname LIKE '%\_default\_viewer\_%'
                   OR polname LIKE '%\_default\_editor\_%'
                   OR polname LIKE '%\_default\_sensor\_%'
                   OR polname LIKE '%\_default\_qc\_%'
                   OR polname LIKE '%\_default\_obs\_manager\_%'
            LOOP
                EXECUTE format(
                    'DROP POLICY IF EXISTS %I ON sensorthings.%I;',
                    r.polname, r.relname
                );
            END LOOP;
        END $$;

        -- --------------------------------------------------------------
        -- 2. Session-claim helper functions.
        --    STABLE (not VOLATILE): the value can't change mid-statement,
        --    which lets the planner treat repeated calls within one query
        --    as free -- the same reasoning Supabase documents for its own
        --    auth.uid() helper.
        -- --------------------------------------------------------------
        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_id()
        RETURNS bigint
        LANGUAGE sql STABLE
        AS $fn$
            SELECT NULLIF(current_setting('app.current_user_id', true), '')::bigint;
        $fn$;

        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_role()
        RETURNS text
        LANGUAGE sql STABLE
        AS $fn$
            SELECT role FROM sensorthings."User"
            WHERE id = sensorthings.current_app_user_id();
        $fn$;

        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_dataset_id()
        RETURNS text
        LANGUAGE sql STABLE
        AS $fn$
            SELECT dataset_id FROM sensorthings."User"
            WHERE id = sensorthings.current_app_user_id();
        $fn$;

        -- --------------------------------------------------------------
        -- 3. Retire the old per-approval policy functions. Nothing calls
        --    these anymore (see the Python-side changes in the same PR).
        -- --------------------------------------------------------------
        DROP FUNCTION IF EXISTS sensorthings.viewer_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.editor_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.obs_manager_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.sensor_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.qc_policy(text[], text);

        -- --------------------------------------------------------------
        -- 4. Static policies, created once, group-role-scoped.
        -- --------------------------------------------------------------
        DO $$
        DECLARE
            tablename text;
            read_tables TEXT[];
        BEGIN
            read_tables := ARRAY[
                'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
                'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation'
            ];
            IF coalesce(current_setting('custom.network', true)::boolean, false) THEN
                read_tables := read_tables || ARRAY['Network'];
            END IF;

            -- ============================================================
            -- "user" group: viewer (read-only) + editor (read/write).
            -- One shared SELECT policy for both roles, plus a
            -- role-gated write policy that only an 'editor' satisfies.
            -- ============================================================
            FOREACH tablename IN ARRAY read_tables
            LOOP
                EXECUTE format(
                    'DROP POLICY IF EXISTS rbac_user_select_%s ON sensorthings.%I;',
                    tablename, tablename
                );
                IF tablename = 'Datastream' THEN
                    EXECUTE format(
                        'CREATE POLICY rbac_user_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "user"
                        USING (dataset_id = sensorthings.current_app_user_dataset_id());',
                        tablename, tablename
                    );
                ELSE
                    EXECUTE format(
                        'CREATE POLICY rbac_user_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "user"
                        USING (TRUE);',
                        tablename, tablename
                    );
                END IF;

                EXECUTE format(
                    'DROP POLICY IF EXISTS rbac_editor_write_%s ON sensorthings.%I;',
                    tablename, tablename
                );
                IF tablename = 'Datastream' THEN
                    EXECUTE format(
                        'CREATE POLICY rbac_editor_write_%s
                        ON sensorthings.%I
                        FOR ALL
                        TO "user"
                        USING (
                            sensorthings.current_app_user_role() = ''editor''
                            AND dataset_id = sensorthings.current_app_user_dataset_id()
                        )
                        WITH CHECK (
                            sensorthings.current_app_user_role() = ''editor''
                            AND dataset_id = sensorthings.current_app_user_dataset_id()
                        );',
                        tablename, tablename
                    );
                ELSE
                    EXECUTE format(
                        'CREATE POLICY rbac_editor_write_%s
                        ON sensorthings.%I
                        FOR ALL
                        TO "user"
                        USING (sensorthings.current_app_user_role() = ''editor'')
                        WITH CHECK (sensorthings.current_app_user_role() = ''editor'');',
                        tablename, tablename
                    );
                END IF;
            END LOOP;

            -- ============================================================
            -- "sensor" group: sensor (field device) + obs_manager.
            -- Same shared-read / role-gated-write shape.
            -- ============================================================
            FOREACH tablename IN ARRAY read_tables
            LOOP
                EXECUTE format(
                    'DROP POLICY IF EXISTS rbac_sensor_select_%s ON sensorthings.%I;',
                    tablename, tablename
                );
                IF tablename = 'Datastream' THEN
                    EXECUTE format(
                        'CREATE POLICY rbac_sensor_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "sensor"
                        USING (dataset_id = sensorthings.current_app_user_dataset_id());',
                        tablename, tablename
                    );
                ELSE
                    EXECUTE format(
                        'CREATE POLICY rbac_sensor_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "sensor"
                        USING (TRUE);',
                        tablename, tablename
                    );
                END IF;
            END LOOP;

            -- sensor-specific writes (field device: append observations,
            -- update its own datastream/location metadata).
            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_observation_insert ON sensorthings."Observation";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_observation_insert
                ON sensorthings."Observation"
                FOR INSERT
                TO "sensor"
                WITH CHECK (sensorthings.current_app_user_role() = 'sensor');
            $q$;

            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_foi_insert ON sensorthings."FeaturesOfInterest";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_foi_insert
                ON sensorthings."FeaturesOfInterest"
                FOR INSERT
                TO "sensor"
                WITH CHECK (sensorthings.current_app_user_role() = 'sensor');
            $q$;

            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_datastream_update ON sensorthings."Datastream";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_datastream_update
                ON sensorthings."Datastream"
                FOR UPDATE
                TO "sensor"
                USING (
                    sensorthings.current_app_user_role() = 'sensor'
                    AND dataset_id = sensorthings.current_app_user_dataset_id()
                )
                WITH CHECK (
                    sensorthings.current_app_user_role() = 'sensor'
                    AND dataset_id = sensorthings.current_app_user_dataset_id()
                );
            $q$;

            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_location_update ON sensorthings."Location";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_location_update
                ON sensorthings."Location"
                FOR UPDATE
                TO "sensor"
                USING (sensorthings.current_app_user_role() = 'sensor')
                WITH CHECK (sensorthings.current_app_user_role() = 'sensor');
            $q$;

            -- obs_manager-specific write: full control of Observation.
            EXECUTE 'DROP POLICY IF EXISTS rbac_obs_manager_observation_all ON sensorthings."Observation";';
            EXECUTE $q$
                CREATE POLICY rbac_obs_manager_observation_all
                ON sensorthings."Observation"
                FOR ALL
                TO "sensor"
                USING (sensorthings.current_app_user_role() = 'obs_manager')
                WITH CHECK (sensorthings.current_app_user_role() = 'obs_manager');
            $q$;

            -- ============================================================
            -- "qc" group: dedicated role, no sharing subtlety.
            -- ============================================================
            FOREACH tablename IN ARRAY read_tables
            LOOP
                EXECUTE format(
                    'DROP POLICY IF EXISTS rbac_qc_select_%s ON sensorthings.%I;',
                    tablename, tablename
                );
                IF tablename = 'Datastream' THEN
                    EXECUTE format(
                        'CREATE POLICY rbac_qc_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "qc"
                        USING (dataset_id = sensorthings.current_app_user_dataset_id());',
                        tablename, tablename
                    );
                ELSE
                    EXECUTE format(
                        'CREATE POLICY rbac_qc_select_%s
                        ON sensorthings.%I
                        FOR SELECT
                        TO "qc"
                        USING (TRUE);',
                        tablename, tablename
                    );
                END IF;
            END LOOP;

            EXECUTE 'DROP POLICY IF EXISTS rbac_qc_observation_update ON sensorthings."Observation";';
            EXECUTE $q$
                CREATE POLICY rbac_qc_observation_update
                ON sensorthings."Observation"
                FOR UPDATE
                TO "qc"
                USING (TRUE)
                WITH CHECK (TRUE);
            $q$;
        END $$;

        RESET ROLE;

    END IF;
END $BODY$;
