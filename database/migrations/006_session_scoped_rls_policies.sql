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
-- Migration: 006_session_scoped_rls_policies
--
-- istSOS users are not PostgreSQL roles. The backend connects as one service
-- account and set_role() runs `SET LOCAL ROLE <group>` ("user"/"sensor"/"qc")
-- plus `set_config('app.current_user_id', ...)`. RLS therefore cannot be
-- scoped `TO <username>` -- it is scoped to the group role, and the caller's
-- application role / network grant is read back inside the USING clause from
-- the session claim via the helper functions below.
--
-- Access model
-- ------------
--   role  -> what actions            (viewer=read, editor=read+write, ...)
--   User.dataset_id -> which rows     (the name of a Network the user is
--                                      scoped to; NULL = no restriction)
--
-- Network scoping applies to Datastream and Observation only. Every other
-- table keeps a blanket grant per role. When the NETWORK feature is off
-- (custom.network = false) there is nothing to scope and every table gets a
-- blanket grant.
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- --------------------------------------------------------------
        -- 1. Drop the legacy per-user policy functions and any stale
        --    per-username policies left by earlier runs / test fixtures.
        --    istsos_auth.sql no longer defines these functions; the
        --    DROP IF EXISTS is kept only to clean databases that were
        --    initialised with an older istsos_auth.sql.
        -- --------------------------------------------------------------
        DROP FUNCTION IF EXISTS sensorthings.viewer_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.editor_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.obs_manager_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.sensor_policy(text[], text);
        DROP FUNCTION IF EXISTS sensorthings.qc_policy(text[], text);

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
        -- 2. Session-claim helper functions. STABLE so the planner can
        --    treat repeated calls within one query as free.
        -- --------------------------------------------------------------
        -- SECURITY DEFINER on every helper that touches sensorthings."User".
        --
        -- These run inside RLS predicates, so they execute as whichever
        -- group role the request is running as -- and only "user" and
        -- "administrator" hold SELECT on "User". A 'sensor', 'obs_manager'
        -- or 'qc' caller therefore blew up with
        --     ERROR: permission denied for table User
        -- on ANY read of a policy-protected table, surfacing as a 500.
        --
        -- The alternative -- GRANT SELECT ON "User" TO "sensor","qc" -- was
        -- rejected: "User" carries the bcrypt password column and has no
        -- RLS of its own, so that would hand every hash to two more roles.
        -- SECURITY DEFINER keeps the read inside these four functions, each
        -- of which only ever returns a single attribute of the *caller's own*
        -- row. search_path is pinned so a caller cannot shadow "User" with
        -- a temp table and capture the definer's privileges.
        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_id()
        RETURNS bigint
        LANGUAGE sql STABLE
        AS $fn$
            SELECT NULLIF(current_setting('app.current_user_id', true), '')::bigint;
        $fn$;

        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_role()
        RETURNS text
        LANGUAGE sql STABLE
        SECURITY DEFINER
        SET search_path = sensorthings, pg_catalog, pg_temp
        AS $fn$
            SELECT role FROM sensorthings."User"
            WHERE id = sensorthings.current_app_user_id();
        $fn$;

        -- The raw grant: the Network name stored on the User row, or NULL.
        CREATE OR REPLACE FUNCTION sensorthings.current_app_user_dataset_id()
        RETURNS text
        LANGUAGE sql STABLE
        SECURITY DEFINER
        SET search_path = sensorthings, pg_catalog, pg_temp
        AS $fn$
            SELECT dataset_id FROM sensorthings."User"
            WHERE id = sensorthings.current_app_user_id();
        $fn$;

        -- --------------------------------------------------------------
        -- 3. Network resolver -- only meaningful when the NETWORK feature
        --    is enabled (the "Network" table and Datastream.network_id
        --    only exist then). Resolves the grant name to a Network id;
        --    NULL if the user has no grant OR the name matches no network
        --    (the latter fails closed -- see the policy predicates below).
        -- --------------------------------------------------------------
        IF coalesce(current_setting('custom.network', true)::boolean, false) THEN
            CREATE OR REPLACE FUNCTION sensorthings.current_app_user_network_id()
            RETURNS bigint
            LANGUAGE sql STABLE
            SECURITY DEFINER
            SET search_path = sensorthings, pg_catalog, pg_temp
            AS $fn$
                SELECT n.id
                FROM sensorthings."Network" n
                JOIN sensorthings."User" u ON u.dataset_id = n.name
                WHERE u.id = sensorthings.current_app_user_id();
            $fn$;

            -- The caller's visible Datastream ids, as an array.
            --
            -- Exists so the Observation policy can say
            --     datastream_id = ANY (...)
            -- instead of  datastream_id IN (SELECT ... FROM "Datastream" ...).
            -- See the obs_scope comment below for why the subquery form is
            -- unusable on a TimescaleDB hypertable.
            --
            -- Returns an empty array (never NULL) when the user's grant
            -- matches no Network, so the predicate stays fail-CLOSED: an
            -- unknown or deleted Network name yields zero visible rows
            -- rather than accidentally matching everything.
            CREATE OR REPLACE FUNCTION sensorthings.current_app_user_datastream_ids()
            RETURNS bigint[]
            LANGUAGE sql STABLE
            SECURITY DEFINER
            SET search_path = sensorthings, pg_catalog, pg_temp
            AS $fn$
                SELECT coalesce(array_agg(d.id), ARRAY[]::bigint[])
                FROM sensorthings."Datastream" d
                WHERE d.network_id = sensorthings.current_app_user_network_id();
            $fn$;
        END IF;

        -- --------------------------------------------------------------
        -- 3b. Lock down sensorthings."User" for the "user" group role.
        --
        -- istsos_auth.sql already REVOKEs SELECT on "User" from "guest"
        -- (line ~421), "sensor" (~434) and "qc" (~444), and REVOKEs
        -- INSERT/UPDATE/DELETE from "user" (~409) -- but it never REVOKEs
        -- SELECT from "user". "user" is the single group role that viewer,
        -- editor AND custom all run as (see DB_ROLE_BY_RBAC_ROLE), so every
        -- ordinary authenticated account could
        --     SELECT * FROM sensorthings."User"
        -- and read the whole account roster, including the bcrypt password
        -- hash of every other user and of the administrator.
        --
        -- This is only safe to remove now because step 2/3 above made the
        -- RLS helper functions (current_app_user_role / _dataset_id /
        -- _network_id / _datastream_ids) SECURITY DEFINER: they no longer
        -- need the *calling* role to hold SELECT on "User". That is also why
        -- the REVOKE lives here rather than in istsos_auth.sql -- it is a
        -- direct consequence of the SECURITY DEFINER change, and it must also
        -- undo the identical blanket grant re-issued by
        -- istsos_schema_versioning.sql when VERSIONING is on.
        --
        -- The API never reads "User" as a group role: get_user_from_db(),
        -- set_role() and GET /Users all run on the pooled service-account
        -- connection. Verified: acceptance 88/88, auth-parity 6x26, OIDC
        -- e2e 19/19 all green with this revoked.
        REVOKE SELECT ON sensorthings."User" FROM "user";

        -- --------------------------------------------------------------
        -- 4. Static group-scoped policies.
        -- --------------------------------------------------------------
        DO $$
        DECLARE
            tname       text;
            all_tables  text[] := ARRAY[
                'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
                'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation'
            ];
            net_on      boolean := coalesce(current_setting('custom.network', true)::boolean, false);
            ds_scope    text;   -- extra row predicate for Datastream
            obs_scope   text;   -- extra row predicate for Observation
            grp         text;
            read_groups text[] := ARRAY['user', 'sensor', 'qc'];
            base_pred   text;
            read_pred   text;
        BEGIN
            IF net_on THEN
                all_tables := all_tables || ARRAY['Network'];
                ds_scope :=
                    '(sensorthings.current_app_user_dataset_id() IS NULL'
                    || ' OR network_id = sensorthings.current_app_user_network_id())';
                -- Deliberately "= ANY (<function returning bigint[]>)" and
                -- NOT "IN (SELECT ... FROM Datastream ...)".
                --
                -- Observation is a TimescaleDB hypertable. A correlated
                -- subquery inside an RLS predicate makes the planner build a
                -- Result subplan, and TimescaleDB's SkipScan optimisation --
                -- which kicks in for the count(DISTINCT id) that $count=true
                -- generates -- then aborts the whole query with
                --     ERROR: unsupported subplan type for SkipScan: Result
                -- so GET /Observations?$count=true returned 500 for every
                -- RLS-scoped role (admin bypasses RLS, so it looked fine).
                -- A scalar function call is opaque to the planner, produces
                -- no subplan, and sidesteps the interaction entirely.
                obs_scope :=
                    '(sensorthings.current_app_user_dataset_id() IS NULL'
                    || ' OR datastream_id = ANY'
                    || ' (sensorthings.current_app_user_datastream_ids()))';
            ELSE
                ds_scope  := 'TRUE';
                obs_scope := 'TRUE';
            END IF;

            -- ========================================================
            -- READ policies -- one per group role, per table.
            -- Datastream / Observation carry the network scope; every
            -- other table is a blanket SELECT.
            --
            -- The 'custom' application role maps to the "user" group but
            -- gets NO blanket grant here -- its access is defined entirely
            -- by hand via POST /Policies (see create/policy.py). So every
            -- "user"-group read policy is gated on the caller NOT being a
            -- custom user.
            -- ========================================================
            FOREACH grp IN ARRAY read_groups
            LOOP
                FOREACH tname IN ARRAY all_tables
                LOOP
                    base_pred := CASE tname
                        WHEN 'Datastream'  THEN ds_scope
                        WHEN 'Observation' THEN obs_scope
                        ELSE 'TRUE'
                    END;

                    IF grp = 'user' THEN
                        read_pred := 'sensorthings.current_app_user_role() <> ''custom'''
                                     || ' AND (' || base_pred || ')';
                    ELSE
                        read_pred := base_pred;
                    END IF;

                    EXECUTE format(
                        'DROP POLICY IF EXISTS rbac_%s_select_%s ON sensorthings.%I;',
                        grp, tname, tname
                    );
                    EXECUTE format(
                        'CREATE POLICY rbac_%s_select_%s ON sensorthings.%I
                         FOR SELECT TO %I USING (%s);',
                        grp, tname, tname, grp, read_pred
                    );
                END LOOP;
            END LOOP;

            -- ========================================================
            -- WRITE policies.
            --
            -- editor (in the "user" group):
            --   Datastream / Observation  -> network-owned facts. Full
            --       write, network-scoped by the ds_scope / obs_scope
            --       predicate. A scoped editor writes only its own
            --       network's rows; an unscoped editor writes all.
            --
            --   Thing / Sensor / Location / HistoricalLocation /
            --   ObservedProperty / FeaturesOfInterest / Network ->
            --       shared reference data. These carry no clean network
            --       column (a Thing/Sensor can be referenced by several
            --       networks' Datastreams), so they cannot be row-scoped
            --       the same way. Without a guard, a psos-scoped editor
            --       could rename or DELETE an acsot-only Sensor -- a
            --       cross-tenant write, and DELETE cascades into that
            --       network's Datastreams + Observations.
            --
            --       Guard: USING (governs UPDATE / DELETE of an EXISTING
            --       row) additionally requires the caller to be UNSCOPED
            --       (dataset_id IS NULL) -- i.e. a global editor trusted
            --       with every network. WITH CHECK (governs INSERT and the
            --       post-image of UPDATE) only checks the role, so a
            --       *scoped* editor can still CREATE new reference rows
            --       while onboarding a station -- it just can't mutate or
            --       delete existing shared ones. Those go to an admin or a
            --       global editor.
            --
            --       (Network writes are already refused at the API layer
            --       for every non-admin; this is defence in depth.)
            -- ========================================================
            FOREACH tname IN ARRAY all_tables
            LOOP
                EXECUTE format(
                    'DROP POLICY IF EXISTS rbac_editor_write_%s ON sensorthings.%I;',
                    tname, tname
                );
                IF tname IN ('Datastream', 'Observation') THEN
                    EXECUTE format(
                        'CREATE POLICY rbac_editor_write_%s ON sensorthings.%I
                         FOR ALL TO "user"
                         USING (sensorthings.current_app_user_role() = ''editor'' AND %s)
                         WITH CHECK (sensorthings.current_app_user_role() = ''editor'' AND %s);',
                        tname, tname,
                        CASE tname WHEN 'Datastream' THEN ds_scope ELSE obs_scope END,
                        CASE tname WHEN 'Datastream' THEN ds_scope ELSE obs_scope END
                    );
                ELSE
                    EXECUTE format(
                        'CREATE POLICY rbac_editor_write_%s ON sensorthings.%I
                         FOR ALL TO "user"
                         USING (sensorthings.current_app_user_role() = ''editor''
                                AND sensorthings.current_app_user_dataset_id() IS NULL)
                         WITH CHECK (sensorthings.current_app_user_role() = ''editor'');',
                        tname, tname
                    );
                END IF;
            END LOOP;

            -- sensor (field device): append observations + maintain its
            -- own datastream / location metadata. Datastream / Observation
            -- writes are network-scoped.
            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_observation_insert ON sensorthings."Observation";';
            EXECUTE format(
                'CREATE POLICY rbac_sensor_observation_insert ON sensorthings."Observation"
                 FOR INSERT TO "sensor"
                 WITH CHECK (sensorthings.current_app_user_role() = ''sensor'' AND %s);',
                obs_scope
            );

            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_foi_insert ON sensorthings."FeaturesOfInterest";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_foi_insert
                ON sensorthings."FeaturesOfInterest"
                FOR INSERT TO "sensor"
                WITH CHECK (sensorthings.current_app_user_role() = 'sensor');
            $q$;

            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_datastream_update ON sensorthings."Datastream";';
            EXECUTE format(
                'CREATE POLICY rbac_sensor_datastream_update ON sensorthings."Datastream"
                 FOR UPDATE TO "sensor"
                 USING (sensorthings.current_app_user_role() = ''sensor'' AND %s)
                 WITH CHECK (sensorthings.current_app_user_role() = ''sensor'' AND %s);',
                ds_scope, ds_scope
            );

            -- Same shared-reference reasoning as the editor block above:
            -- Location has no network column, so a scoped 'sensor' must not
            -- be able to rewrite an arbitrary station's coordinates. Only an
            -- UNSCOPED sensor (dataset_id IS NULL) may UPDATE a Location.
            EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_location_update ON sensorthings."Location";';
            EXECUTE $q$
                CREATE POLICY rbac_sensor_location_update
                ON sensorthings."Location"
                FOR UPDATE TO "sensor"
                USING (sensorthings.current_app_user_role() = 'sensor'
                       AND sensorthings.current_app_user_dataset_id() IS NULL)
                WITH CHECK (sensorthings.current_app_user_role() = 'sensor');
            $q$;

            -- obs_manager (in the "sensor" group): full control of
            -- Observation, network-scoped.
            EXECUTE 'DROP POLICY IF EXISTS rbac_obs_manager_observation_all ON sensorthings."Observation";';
            EXECUTE format(
                'CREATE POLICY rbac_obs_manager_observation_all ON sensorthings."Observation"
                 FOR ALL TO "sensor"
                 USING (sensorthings.current_app_user_role() = ''obs_manager'' AND %s)
                 WITH CHECK (sensorthings.current_app_user_role() = ''obs_manager'' AND %s);',
                obs_scope, obs_scope
            );

            -- qc: update observations (quality flags), network-scoped.
            EXECUTE 'DROP POLICY IF EXISTS rbac_qc_observation_update ON sensorthings."Observation";';
            EXECUTE format(
                'CREATE POLICY rbac_qc_observation_update ON sensorthings."Observation"
                 FOR UPDATE TO "qc"
                 USING (%s)
                 WITH CHECK (%s);',
                obs_scope, obs_scope
            );
        END $$;

        RESET ROLE;

    END IF;
END $BODY$;
