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
-- Migration: 011_guest_reference_data_only
-- Description: With ANONYMOUS_VIEWER=1 an unauthenticated read runs as the
--              "guest" PostgreSQL role. istsos_auth.sql gives guest a
--              USING (true) SELECT policy on every entity table, which made
--              an anonymous caller the only role able to read every
--              Network's Datastreams and Observations -- more than a
--              logged-in, Network-scoped viewer.
--
--              This drops guest's policies on Datastream, Observation and
--              Network. Guest keeps read access to the shared reference
--              tables (Location, Thing, HistoricalLocation, ObservedProperty,
--              Sensor, FeaturesOfInterest), which are unscoped for every
--              role. With no policy, RLS returns no rows to guest on the
--              dropped tables: lists come back empty, reads by id are 404.
--
--              Idempotent (DROP POLICY IF EXISTS).
-- =============================================================================

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        DROP POLICY IF EXISTS anonymous_Datastream ON sensorthings."Datastream";
        DROP POLICY IF EXISTS anonymous_Observation ON sensorthings."Observation";

        IF to_regclass('sensorthings."Network"') IS NOT NULL THEN
            DROP POLICY IF EXISTS anonymous_Network ON sensorthings."Network";
        END IF;

        RESET ROLE;

    END IF;
END $BODY$;
