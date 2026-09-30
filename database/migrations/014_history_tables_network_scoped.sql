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
-- Migration: 014_history_tables_network_scoped
-- Description: sensorthings_history.* tables (VERSIONING=1) had no row-level
--              security at all -- only a blanket GRANT SELECT per role
--              (istsos_schema_versioning.sql). The *_traveltime views that
--              back $as_of are UNION(live table, history table) with
--              security_invoker = on, so they already inherited whatever
--              RLS the live table enforced -- but the history arm of that
--              UNION enforced nothing. A Network-scoped viewer could not
--              see another Network's current Datastream/Observation, but
--              could see an old (edited or deleted) version of the same row
--              through $as_of, entirely bypassing the scoping
--              006_session_scoped_rls_policies.sql added.
--
--              This mirrors 006's read-policy loop onto the history tables:
--              same predicates (ds_scope / obs_scope), same roles, same
--              custom-role gate, same reference-table guest access (only,
--              per 011_guest_reference_data_only.sql -- Datastream/
--              Observation/Network history stay closed to guest).
--
--              History tables are insert-only from the API's point of view
--              (istsos_mutate_history() writes them; a trigger blocks
--              UPDATE/DELETE), but that INSERT still runs as the caller's
--              own role -- istsos_mutate_history() is not SECURITY DEFINER
--              -- so enabling RLS also needs INSERT policies, or the
--              trigger's own write is rejected and every scoped write to a
--              versioned table starts failing. This adds a WITH CHECK
--              (true) INSERT policy per role, but ONLY on the tables that
--              role already has a table-level INSERT GRANT on
--              (istsos_schema_versioning.sql): "user" on all of them,
--              "sensor" only on Datastream, "qc" only on Observation. That
--              reproduces exactly the pre-migration, grant-only write
--              surface -- this migration only ever narrows SELECT, never
--              widens INSERT. Gaps in that grant list itself (e.g. sensor
--              has no INSERT grant on Location/Observation history, a
--              pre-existing upstream issue) are unchanged by this
--              migration and out of scope for it.
--
--              administrator owns these tables and bypasses RLS as owner,
--              same as the live tables (RLS is enabled, not FORCEd -- a
--              pre-existing, documented characteristic of this schema, not
--              introduced here).
--
--              Idempotent: DROP POLICY IF EXISTS before each CREATE; each
--              table is checked with to_regclass before use. Rerunning this
--              migration (e.g. after this file was corrected) safely
--              replaces its own policies; it never touches any other
--              policy.
-- =============================================================================

DO $BODY$
DECLARE
    tname       text;
    all_tables  text[] := ARRAY[
        'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
        'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation'
    ];
    reference_tables text[] := ARRAY[
        'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
        'Sensor', 'FeaturesOfInterest'
    ];
    net_on      boolean := coalesce(current_setting('custom.network', true)::boolean, false);
    ds_scope    text;
    obs_scope   text;
    grp         text;
    read_groups text[] := ARRAY['user', 'sensor', 'qc'];
    base_pred   text;
    read_pred   text;
    -- Tables each group already has a table-level INSERT GRANT on for
    -- sensorthings_history (istsos_schema_versioning.sql), i.e. the tables
    -- whose versioning trigger that group must still be able to write.
    insert_tables_user   text[] := ARRAY[
        'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
        'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation', 'Network'
    ];
    insert_tables_sensor text[] := ARRAY['Datastream'];
    insert_tables_qc     text[] := ARRAY['Observation'];
BEGIN
    IF current_setting('custom.authorization', true)::boolean
       AND coalesce(current_setting('custom.versioning', true)::boolean, false) THEN

        SET ROLE "administrator";

        IF net_on THEN
            all_tables := all_tables || ARRAY['Network'];
            ds_scope :=
                '(sensorthings.current_app_user_dataset_id() IS NULL'
                || ' OR network_id = sensorthings.current_app_user_network_id())';
            obs_scope :=
                '(sensorthings.current_app_user_dataset_id() IS NULL'
                || ' OR datastream_id = ANY'
                || ' (sensorthings.current_app_user_datastream_ids()))';
        ELSE
            ds_scope  := 'TRUE';
            obs_scope := 'TRUE';
        END IF;

        FOREACH tname IN ARRAY all_tables
        LOOP
            IF to_regclass(format('sensorthings_history.%I', tname)) IS NULL THEN
                CONTINUE;
            END IF;

            EXECUTE format('ALTER TABLE sensorthings_history.%I ENABLE ROW LEVEL SECURITY;', tname);

            FOREACH grp IN ARRAY read_groups
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
                    'DROP POLICY IF EXISTS rbac_history_%s_select_%s ON sensorthings_history.%I;',
                    grp, tname, tname
                );
                EXECUTE format(
                    'CREATE POLICY rbac_history_%s_select_%s ON sensorthings_history.%I
                     FOR SELECT TO %I USING (%s);',
                    grp, tname, tname, grp, read_pred
                );
            END LOOP;

            IF tname = ANY(reference_tables) THEN
                EXECUTE format(
                    'DROP POLICY IF EXISTS anonymous_history_%s ON sensorthings_history.%I;',
                    tname, tname
                );
                EXECUTE format(
                    'CREATE POLICY anonymous_history_%s ON sensorthings_history.%I
                     FOR SELECT TO "guest" USING (true);',
                    tname, tname
                );
            END IF;

            -- INSERT policies for istsos_mutate_history()'s own write,
            -- restricted to exactly the pre-existing GRANT surface.
            IF tname = ANY(insert_tables_user) THEN
                EXECUTE format('DROP POLICY IF EXISTS rbac_history_user_insert_%s ON sensorthings_history.%I;', tname, tname);
                EXECUTE format('CREATE POLICY rbac_history_user_insert_%s ON sensorthings_history.%I FOR INSERT TO "user" WITH CHECK (true);', tname, tname);
            END IF;
            IF tname = ANY(insert_tables_sensor) THEN
                EXECUTE format('DROP POLICY IF EXISTS rbac_history_sensor_insert_%s ON sensorthings_history.%I;', tname, tname);
                EXECUTE format('CREATE POLICY rbac_history_sensor_insert_%s ON sensorthings_history.%I FOR INSERT TO "sensor" WITH CHECK (true);', tname, tname);
            END IF;
            IF tname = ANY(insert_tables_qc) THEN
                EXECUTE format('DROP POLICY IF EXISTS rbac_history_qc_insert_%s ON sensorthings_history.%I;', tname, tname);
                EXECUTE format('CREATE POLICY rbac_history_qc_insert_%s ON sensorthings_history.%I FOR INSERT TO "qc" WITH CHECK (true);', tname, tname);
            END IF;
        END LOOP;

        RESET ROLE;

    END IF;
END $BODY$;
