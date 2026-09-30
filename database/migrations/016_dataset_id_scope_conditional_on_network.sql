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
-- Migration: 016_dataset_id_scope_conditional_on_network
-- Description: Two RLS predicates from 006_session_scoped_rls_policies.sql
--              (one later widened by 015) hardcoded
--              "AND current_app_user_dataset_id() IS NULL" unconditionally,
--              regardless of whether NETWORK is on:
--                - rbac_editor_write_<table> for every shared-reference
--                  table (Location, Thing, HistoricalLocation,
--                  ObservedProperty, Sensor, FeaturesOfInterest[, Network])
--                - rbac_sensor_location_update
--
--              Every OTHER network-scoped predicate (ds_scope / obs_scope,
--              gating Datastream and Observation) already correctly
--              collapses to unconditional TRUE when NETWORK=0. These two
--              were the only ones left checking dataset_id even in that
--              mode, so an editor/sensor/obs_manager with any non-NULL
--              dataset_id got 403 on every write to a shared-reference
--              table -- with NETWORK=0, that should never happen.
--
--              This is currently masked, not triggered: a separate fix
--              (register_request.py, oidc_login.py, admin_approval.py,
--              role_crud.py, create/user.py) already makes every entry
--              point force dataset_id to NULL whenever NETWORK=0, so the
--              IS NULL check happens to always pass today. This migration
--              makes the RLS layer defend itself too, instead of relying
--              solely on the application layer's guarantee -- the same
--              defence-in-depth reasoning 006 already applies elsewhere
--              ("Network writes are already refused at the API layer for
--              every non-admin; this is defence in depth").
--
--              Idempotent: DROP POLICY IF EXISTS before each CREATE.
-- =============================================================================

DO $BODY$
DECLARE
    tname         text;
    shared_tables text[] := ARRAY[
        'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
        'Sensor', 'FeaturesOfInterest'
    ];
    net_on        boolean := coalesce(current_setting('custom.network', true)::boolean, false);
    scope_guard   text;
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        IF net_on THEN
            shared_tables := shared_tables || ARRAY['Network'];
        END IF;

        scope_guard := CASE WHEN net_on
            THEN 'sensorthings.current_app_user_dataset_id() IS NULL'
            ELSE 'TRUE'
        END;

        FOREACH tname IN ARRAY shared_tables
        LOOP
            EXECUTE format(
                'DROP POLICY IF EXISTS rbac_editor_write_%s ON sensorthings.%I;',
                tname, tname
            );
            EXECUTE format(
                'CREATE POLICY rbac_editor_write_%s ON sensorthings.%I
                 FOR ALL TO "user"
                 USING (sensorthings.current_app_user_role() = ''editor'' AND %s)
                 WITH CHECK (sensorthings.current_app_user_role() = ''editor'');',
                tname, tname, scope_guard
            );
        END LOOP;

        -- Same predicate, widened for 'sensor'/'obs_manager' by 015 --
        -- reapplied here with the net_on guard on top of that widening.
        EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_location_update ON sensorthings."Location";';
        EXECUTE format(
            'CREATE POLICY rbac_sensor_location_update
             ON sensorthings."Location"
             FOR UPDATE TO "sensor"
             USING (sensorthings.current_app_user_role() IN (''sensor'', ''obs_manager'')
                    AND %s)
             WITH CHECK (sensorthings.current_app_user_role() IN (''sensor'', ''obs_manager''));',
            scope_guard
        );

        RESET ROLE;

    END IF;
END $BODY$;
