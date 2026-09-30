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
-- Migration: 010_unscoped_writes_without_network
-- Description: When the NETWORK feature is off, stop User.dataset_id from
--              restricting writes to shared reference tables.
--
--              006_session_scoped_rls_policies.sql lets an editor UPDATE /
--              DELETE an existing Location, Thing, HistoricalLocation,
--              ObservedProperty, Sensor or FeaturesOfInterest row -- and a
--              sensor UPDATE a Location -- only when the caller's
--              dataset_id IS NULL. That guard exists to stop a
--              Network-scoped user editing another Network's shared rows,
--              but 006 also emitted it with NETWORK=0, where there are no
--              Networks and dataset_id is never validated. Any editor or
--              sensor whose row carried a dataset_id string was then refused
--              every such write (403).
--
--              With NETWORK=0 this recreates those policies without the
--              guard. With NETWORK=1 it does nothing: the guard is correct.
--              Idempotent (DROP POLICY IF EXISTS before each CREATE).
-- =============================================================================

DO $BODY$
DECLARE
    tname text;
BEGIN
    IF current_setting('custom.authorization', true)::boolean
       AND NOT coalesce(current_setting('custom.network', true)::boolean, false) THEN

        SET ROLE "administrator";

        FOREACH tname IN ARRAY ARRAY[
            'Location', 'Thing', 'HistoricalLocation', 'ObservedProperty',
            'Sensor', 'FeaturesOfInterest'
        ]
        LOOP
            EXECUTE format(
                'DROP POLICY IF EXISTS rbac_editor_write_%s ON sensorthings.%I;',
                tname, tname
            );
            EXECUTE format(
                'CREATE POLICY rbac_editor_write_%s ON sensorthings.%I
                 FOR ALL TO "user"
                 USING (sensorthings.current_app_user_role() = ''editor'')
                 WITH CHECK (sensorthings.current_app_user_role() = ''editor'');',
                tname, tname
            );
        END LOOP;

        DROP POLICY IF EXISTS rbac_sensor_location_update ON sensorthings."Location";
        CREATE POLICY rbac_sensor_location_update
        ON sensorthings."Location"
        FOR UPDATE TO "sensor"
        USING (sensorthings.current_app_user_role() = 'sensor')
        WITH CHECK (sensorthings.current_app_user_role() = 'sensor');

        RESET ROLE;

    END IF;
END $BODY$;
