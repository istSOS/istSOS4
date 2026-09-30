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
-- Migration: 015_obs_manager_foi_datastream_location
-- Description: obs_manager lost three of the five grants it had under
--              upstream's old per-user sensorthings.obs_manager_policy()
--              (istsos_auth.sql, still live on upstream/main today):
--              INSERT on FeaturesOfInterest, UPDATE on Datastream, UPDATE
--              on Location. 006_session_scoped_rls_policies.sql replaced
--              every per-user policy function with static, group-shared
--              policies -- for obs_manager it only recreated the
--              Observation grant (rbac_obs_manager_observation_all);
--              rbac_sensor_foi_insert / rbac_sensor_datastream_update /
--              rbac_sensor_location_update were written checking
--              current_app_user_role() = 'sensor' only, silently excluding
--              obs_manager even though both roles share the "sensor"
--              PostgreSQL group.
--
--              This widens those three policies' role check to
--              ('sensor', 'obs_manager'). Every other part of each
--              predicate -- ds_scope on Datastream, the "unscoped only" on
--              Location, no scope at all on FeaturesOfInterest -- is
--              copied unchanged from 006, so obs_manager ends up with
--              exactly the same Network-scoped version of these three
--              grants that sensor already has, rather than upstream's
--              original unscoped ones (upstream's version predates Network
--              scoping entirely).
--
--              rbac_sensor_observation_insert, rbac_qc_observation_update
--              and rbac_obs_manager_observation_all are untouched --
--              obs_manager's own Observation grant was never missing.
--
--              Idempotent: DROP POLICY IF EXISTS before each CREATE.
-- =============================================================================

DO $BODY$
DECLARE
    net_on    boolean := coalesce(current_setting('custom.network', true)::boolean, false);
    ds_scope  text;
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        IF net_on THEN
            ds_scope :=
                '(sensorthings.current_app_user_dataset_id() IS NULL'
                || ' OR network_id = sensorthings.current_app_user_network_id())';
        ELSE
            ds_scope := 'TRUE';
        END IF;

        EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_foi_insert ON sensorthings."FeaturesOfInterest";';
        EXECUTE $q$
            CREATE POLICY rbac_sensor_foi_insert
            ON sensorthings."FeaturesOfInterest"
            FOR INSERT TO "sensor"
            WITH CHECK (sensorthings.current_app_user_role() IN ('sensor', 'obs_manager'));
        $q$;

        EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_datastream_update ON sensorthings."Datastream";';
        EXECUTE format(
            'CREATE POLICY rbac_sensor_datastream_update ON sensorthings."Datastream"
             FOR UPDATE TO "sensor"
             USING (sensorthings.current_app_user_role() IN (''sensor'', ''obs_manager'') AND %s)
             WITH CHECK (sensorthings.current_app_user_role() IN (''sensor'', ''obs_manager'') AND %s);',
            ds_scope, ds_scope
        );

        EXECUTE 'DROP POLICY IF EXISTS rbac_sensor_location_update ON sensorthings."Location";';
        EXECUTE $q$
            CREATE POLICY rbac_sensor_location_update
            ON sensorthings."Location"
            FOR UPDATE TO "sensor"
            USING (sensorthings.current_app_user_role() IN ('sensor', 'obs_manager')
                   AND sensorthings.current_app_user_dataset_id() IS NULL)
            WITH CHECK (sensorthings.current_app_user_role() IN ('sensor', 'obs_manager'));
        $q$;

        RESET ROLE;

    END IF;
END $BODY$;
