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
-- Migration: 010_observation_dataset_scoping
--
-- The gap this closes
-- --------------------
-- 007_session_scoped_rls_policies.sql dataset-scoped Datastream for viewer,
-- editor, sensor, obs_manager, and qc -- but its own header comment says
-- plainly that Observation was left unscoped on purpose, deferred:
-- "dataset_id only exists as a column on Datastream ... every other table
-- keeps a blanket USING (TRUE) for these roles ... Extending dataset_id to
-- other tables is future work, not attempted here."
--
-- Verified against a live database: a viewer approved into one dataset
-- (scoped to 2 of ~20 Datastreams) could still read every Observation in
-- the entire table -- the actual measurement values, not just the
-- Datastream label describing them. That's the real payload this system
-- is meant to be governing access to, so this is a genuine confidentiality
-- gap, not merely a cosmetic inconsistency with Datastream's own scoping.
--
-- This also touches the *write* policies, which read-only testing wouldn't
-- surface: rbac_editor_write_observation, rbac_sensor_observation_insert,
-- rbac_obs_manager_observation_all, and rbac_qc_observation_update are each
-- independent RLS policies with their own USING/WITH CHECK clauses, none
-- referencing dataset_id. PostgreSQL ORs permissive policies together, so
-- scoping only the SELECT policy would leave an editor able to UPDATE or
-- DELETE an Observation by id they can no longer see in a collection
-- browse. Both sides need to close together for this to mean anything.
--
-- odrl_governed is deliberately left untouched, matching 007's own
-- precedent for that role (it has zero effective policies today and is
-- out of scope for this pass, same as it was for 007).
--
-- The technique
-- -------------
-- Not new: this is the exact correlated-subquery pattern
-- anonymous_observation (004_public_access.sql) already uses in
-- production for the guest role, checking Datastream.dataset_id instead
-- of Datastream.is_public:
--
--     datastream_id IN (
--         SELECT id FROM sensorthings."Datastream"
--         WHERE dataset_id = sensorthings.current_app_user_dataset_id()
--     )
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- ============================================================
        -- "user" group: viewer (read-only) + editor (read/write).
        -- ============================================================
        DROP POLICY IF EXISTS rbac_user_select_observation ON sensorthings."Observation";
        CREATE POLICY rbac_user_select_observation
            ON sensorthings."Observation"
            FOR SELECT
            TO "user"
            USING (
                datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        DROP POLICY IF EXISTS rbac_editor_write_observation ON sensorthings."Observation";
        CREATE POLICY rbac_editor_write_observation
            ON sensorthings."Observation"
            FOR ALL
            TO "user"
            USING (
                sensorthings.current_app_user_role() = 'editor'
                AND datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            )
            WITH CHECK (
                sensorthings.current_app_user_role() = 'editor'
                AND datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        -- ============================================================
        -- "sensor" group: sensor (field device) + obs_manager.
        -- ============================================================
        DROP POLICY IF EXISTS rbac_sensor_select_observation ON sensorthings."Observation";
        CREATE POLICY rbac_sensor_select_observation
            ON sensorthings."Observation"
            FOR SELECT
            TO "sensor"
            USING (
                datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        DROP POLICY IF EXISTS rbac_sensor_observation_insert ON sensorthings."Observation";
        CREATE POLICY rbac_sensor_observation_insert
            ON sensorthings."Observation"
            FOR INSERT
            TO "sensor"
            WITH CHECK (
                sensorthings.current_app_user_role() = 'sensor'
                AND datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        DROP POLICY IF EXISTS rbac_obs_manager_observation_all ON sensorthings."Observation";
        CREATE POLICY rbac_obs_manager_observation_all
            ON sensorthings."Observation"
            FOR ALL
            TO "sensor"
            USING (
                sensorthings.current_app_user_role() = 'obs_manager'
                AND datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            )
            WITH CHECK (
                sensorthings.current_app_user_role() = 'obs_manager'
                AND datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        -- ============================================================
        -- "qc" group: dedicated role.
        -- ============================================================
        DROP POLICY IF EXISTS rbac_qc_select_observation ON sensorthings."Observation";
        CREATE POLICY rbac_qc_select_observation
            ON sensorthings."Observation"
            FOR SELECT
            TO "qc"
            USING (
                datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        DROP POLICY IF EXISTS rbac_qc_observation_update ON sensorthings."Observation";
        CREATE POLICY rbac_qc_observation_update
            ON sensorthings."Observation"
            FOR UPDATE
            TO "qc"
            USING (
                datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            )
            WITH CHECK (
                datastream_id IN (
                    SELECT id FROM sensorthings."Datastream"
                    WHERE dataset_id = sensorthings.current_app_user_dataset_id()
                )
            );

        RESET ROLE;

    END IF;
END $BODY$;
