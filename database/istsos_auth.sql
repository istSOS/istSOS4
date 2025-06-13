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

DO $BODY$
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        -- Create the "User" table if it doesn't exist
        CREATE TABLE IF NOT EXISTS sensorthings."User"( 
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "username" VARCHAR(255) UNIQUE NOT NULL,
            "contact" jsonb DEFAULT NULL,
            "uri" VARCHAR(255),
            "role" VARCHAR(255) NOT NULL
        );

        EXECUTE format('
            CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."User")
            RETURNS text AS $$
                SELECT ''/Users('' || $1.id || '')'';
            $$ LANGUAGE SQL;
        ');

        INSERT INTO sensorthings."User" ("username", "role")
        VALUES (current_setting('custom.user'), 'administrator');

        UPDATE sensorthings."User"
        SET "uri" = '/Users(' || id || ')'
        WHERE "username" = current_setting('custom.user');

        CREATE TABLE IF NOT EXISTS sensorthings."Network" (
            "name" VARCHAR(255) NOT NULL PRIMARY KEY
        );

        INSERT INTO sensorthings."Network" ("name")  
        VALUES
            ('psos'),
            ('acsot'),
            ('defmin');

        CREATE TABLE IF NOT EXISTS sensorthings."Commit"(
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "author" VARCHAR(255) NOT NULL,
            "encodingType" VARCHAR(100),
            "message" VARCHAR(255) NOT NULL,
            "date" TIMESTAMPTZ DEFAULT NOW(),
            "actionType" VARCHAR(100) NOT NULL CHECK ("actionType" IN ('CREATE', 'UPDATE', 'DELETE'))
        );

        -- Create or replace function for selfLink
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT ''/Commits('' || $1.id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the Location table to add the commit_id column
        ALTER TABLE sensorthings."Location" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT NOT NULL
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for Location table
        CREATE INDEX IF NOT EXISTS "idx_location_commit_id" 
        ON sensorthings."Location" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for Location table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location")
            RETURNS text AS $$
                SELECT ''/Locations('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the Thing table to add the commit_id column
        ALTER TABLE sensorthings."Thing" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT NOT NULL
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for Thing table
        CREATE INDEX IF NOT EXISTS "idx_thing_commit_id" 
        ON sensorthings."Thing" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for Thing table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing")
            RETURNS text AS $$
                SELECT ''/Things('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the HistoricalLocation table to add the commit_id column
        ALTER TABLE sensorthings."HistoricalLocation" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT NOT NULL
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for HistoricalLocation table
        CREATE INDEX IF NOT EXISTS "idx_historicallocation_commit_id" 
        ON sensorthings."HistoricalLocation" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for HistoricalLocation table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation")
            RETURNS text AS $$
                SELECT ''/HistoricalLocations('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the ObservedProperty table to add the commit_id column
        ALTER TABLE sensorthings."ObservedProperty" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT NOT NULL
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for ObservedProperty table
        CREATE INDEX IF NOT EXISTS "idx_observedproperty_commit_id" 
        ON sensorthings."ObservedProperty" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for ObservedProperty table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty")
            RETURNS text AS $$
                SELECT ''/ObservedProperties('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the Sensor table to add the commit_id column
        ALTER TABLE sensorthings."Sensor" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT NOT NULL
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for Sensor table
        CREATE INDEX IF NOT EXISTS "idx_sensor_commit_id" 
        ON sensorthings."Sensor" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for Sensor table
        EXECUTE FORMAT ('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor")
            RETURNS text AS $$
                SELECT ''/Sensors('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the Datastream table to add the commit_id column
        ALTER TABLE sensorthings."Datastream" 
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
            ADD COLUMN IF NOT EXISTS "network" VARCHAR(255) NOT NULL REFERENCES sensorthings."Network"(name);
        
        -- Create an index on the commit_id column for Datastream table
        CREATE INDEX IF NOT EXISTS "idx_datastream_commit_id" 
        ON sensorthings."Datastream" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for Datastream table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream")
            RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;
        ');

        -- Alter the FeaturesOfInterest table to add the commit_id column
        ALTER TABLE sensorthings."FeaturesOfInterest" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for FeaturesOfInterest table
        CREATE INDEX IF NOT EXISTS "idx_featuresofinterest_commit_id" 
        ON sensorthings."FeaturesOfInterest" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for FeaturesOfInterest table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest")
            RETURNS text AS $$
                SELECT ''/FeaturesOfInterest('' || $1.id || '')/Commit('' || $1.commit_id || '')'';
            $$ LANGUAGE SQL;'
        );

        -- Alter the Observation table to add the commit_id column
        ALTER TABLE sensorthings."Observation" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;
        
        -- Create an index on the commit_id column for Observation table
        CREATE INDEX IF NOT EXISTS "idx_observation_commit_id" 
        ON sensorthings."Observation" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;
        
        -- Create or replace function for Commit@iot.navigationLink for Observation table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation")
            RETURNS text AS $$
                SELECT CASE
                    WHEN $1.commit_id IS NOT NULL THEN 
                        ''/Observations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for Things@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."Thing" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/Things''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for Locations@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."Location" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/Locations''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for HistoricalLocations@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."HistoricalLocation" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/HistoricalLocations''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for ObservedProperties@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "ObservedProperties@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."ObservedProperty" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/ObservedProperties''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for Sensors@iot.navigationLink in Commit table
         EXECUTE format('
            CREATE OR REPLACE FUNCTION "Sensors@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."Sensor" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/Sensors''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for Datastreams@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."Datastream" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/Datastreams''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');
        -- Create or replace function for FeaturesOfInterest@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "FeaturesOfInterest@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."FeaturesOfInterest" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/FeaturesOfInterest''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        -- Create or replace function for Observations@iot.navigationLink in Commit table
        EXECUTE format('
            CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Commit")
            RETURNS text AS $$
                SELECT CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM sensorthings."Observation" 
                        WHERE commit_id = $1.id
                    ) THEN 
                        ''/Commits('' || $1.id || '')/Observations''
                    ELSE 
                        NULL
                END;
            $$ LANGUAGE SQL;
        ');

        ALTER TABLE sensorthings."Commit"
        ADD COLUMN "user_id" BIGINT NOT NULL REFERENCES sensorthings."User"(id) ON DELETE CASCADE;

        RESET ROLE;

         -- Grant permissions to the administrator role
        GRANT ALL PRIVILEGES ON TABLE sensorthings."Commit" TO "administrator";
        GRANT ALL PRIVILEGES ON SEQUENCE sensorthings."Commit_id_seq" TO "administrator";

        -- Create roles for user
        CREATE ROLE "user";
        GRANT USAGE ON SCHEMA sensorthings TO "user";
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sensorthings TO "user";
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sensorthings TO "user";
        REVOKE INSERT, UPDATE, DELETE ON sensorthings."User" FROM "user";
        REVOKE UPDATE, DELETE ON sensorthings."Commit" FROM "user";
        GRANT "user" TO "administrator" WITH ADMIN OPTION;

        -- Create roles for guest
        CREATE ROLE "guest";
        GRANT USAGE ON SCHEMA sensorthings TO "guest";
        GRANT SELECT ON ALL TABLES IN SCHEMA sensorthings TO "guest";
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sensorthings TO "guest";
        REVOKE SELECT ON sensorthings."User" FROM "guest";
        GRANT "guest" TO "administrator" WITH ADMIN OPTION;

        -- Create roles for sensor
        CREATE ROLE "sensor";
        GRANT USAGE ON SCHEMA sensorthings TO "sensor";
        GRANT SELECT ON ALL TABLES IN SCHEMA sensorthings TO "sensor";
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sensorthings TO "sensor";
        GRANT INSERT, UPDATE, DELETE ON TABLE sensorthings."Observation" TO "sensor";
        GRANT INSERT ON TABLE sensorthings."FeaturesOfInterest" TO "sensor";
        GRANT INSERT ON TABLE sensorthings."Commit" TO "sensor";
        GRANT UPDATE ("phenomenonTime", "last_foi_id", "observedArea") ON sensorthings."Datastream" TO "sensor";
        GRANT UPDATE ("gen_foi_id") ON sensorthings."Location" TO "sensor";
        REVOKE SELECT ON sensorthings."User" FROM "sensor";
        GRANT "sensor" TO "administrator" WITH ADMIN OPTION;

        SET ROLE "administrator";
        
        -- Enable row level security
        ALTER TABLE sensorthings."Location" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."Thing" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."HistoricalLocation" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."ObservedProperty" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."Sensor" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."Datastream" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."FeaturesOfInterest" ENABLE ROW LEVEL SECURITY;
        ALTER TABLE sensorthings."Observation" ENABLE ROW LEVEL SECURITY;

        -- Create policies for row level security
        DO $$ 
        DECLARE 
            tablename TEXT;
        BEGIN
            FOR tablename IN 
                SELECT unnest(ARRAY[
                    'Location', 
                    'Thing', 
                    'HistoricalLocation', 
                    'ObservedProperty', 
                    'Sensor', 
                    'Datastream', 
                    'FeaturesOfInterest', 
                    'Observation'
                ])
            LOOP
                EXECUTE format(
                    'CREATE POLICY anonymous_%s ON sensorthings.%I FOR SELECT TO "guest" USING (true);', 
                    tablename, 
                    tablename
                );
            END LOOP;
        END $$;

        CREATE OR REPLACE FUNCTION sensorthings.viewer_policy(users_ text[], policyname_ text)
        RETURNS void AS $$
        DECLARE
            tablename text;
            user_list_ text;
        BEGIN
            user_list_ := array_to_string(users_, ', ');
            FOR tablename IN
                SELECT unnest(ARRAY[
                    'Location', 
                    'Thing', 
                    'HistoricalLocation', 
                    'ObservedProperty', 
                    'Sensor', 
                    'Datastream', 
                    'FeaturesOfInterest', 
                    'Observation'
                ])
            LOOP
                EXECUTE format(
                    'CREATE POLICY %s_viewer_%s
                    ON sensorthings.%I
                    FOR SELECT
                    TO %s
                    USING (TRUE);',
                    policyname_, tablename, tablename, user_list_
                );
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION sensorthings.editor_policy(users_ text[], policyname_ text)
        RETURNS void AS $$
        DECLARE
            tablename text;
            user_list_ text;
        BEGIN
            user_list_ := array_to_string(users_, ', ');
            FOR tablename IN
                SELECT unnest(ARRAY[
                    'Location', 
                    'Thing', 
                    'HistoricalLocation', 
                    'ObservedProperty', 
                    'Sensor', 
                    'Datastream', 
                    'FeaturesOfInterest', 
                    'Observation'
                ])
            LOOP
                EXECUTE format(
                    'CREATE POLICY %s_editor_%s
                    ON sensorthings.%I
                    FOR ALL
                    TO %s
                    USING (TRUE);',
                    policyname_, tablename, tablename, user_list_
                );
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION sensorthings.sensor_policy(users_ text[], policyname_ text)
        RETURNS void AS $$
        DECLARE
            tablename text;
            user_list_ text;
        BEGIN
            user_list_ := array_to_string(users_, ', ');
            FOR tablename IN
                SELECT unnest(ARRAY[
                    'Location', 
                    'Thing', 
                    'HistoricalLocation', 
                    'ObservedProperty', 
                    'Sensor', 
                    'Datastream', 
                    'FeaturesOfInterest', 
                    'Observation'
                ])
            LOOP
                EXECUTE format(
                    'CREATE POLICY %s_sensor_%s_select
                    ON sensorthings.%I
                    FOR SELECT
                    TO %s
                    USING (TRUE);',
                    policyname_, tablename, tablename, user_list_
                );
            END LOOP;

            EXECUTE format(
                'CREATE POLICY %s_sensor_observation_insert
                ON sensorthings."Observation"
                FOR INSERT
                TO %s
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_sensor_featuresofinterest_insert
                ON sensorthings."FeaturesOfInterest"
                FOR INSERT
                TO %s
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_sensor_datastream_update
                ON sensorthings."Datastream"
                FOR UPDATE
                TO %s
                USING (TRUE)
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_sensor_location_update
                ON sensorthings."Location"
                FOR UPDATE
                TO %s
                USING (TRUE)
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION sensorthings.obs_manager_policy(users_ text[], policyname_ text)
        RETURNS void AS $$
        DECLARE
            tablename text;
            user_list_ text;
        BEGIN
            user_list_ := array_to_string(users_, ', ');
            FOR tablename IN
                SELECT unnest(ARRAY[
                    'Location', 
                    'Thing', 
                    'HistoricalLocation', 
                    'ObservedProperty', 
                    'Sensor', 
                    'Datastream', 
                    'FeaturesOfInterest'
                ])
            LOOP
                EXECUTE format(
                    'CREATE POLICY %s_obs_manager_%s_select
                    ON sensorthings.%I
                    FOR SELECT
                    TO %s
                    USING (TRUE);',
                    policyname_, tablename, tablename, user_list_
                );
            END LOOP;

            EXECUTE format(
                'CREATE POLICY %s_obs_manager_observation_all
                ON sensorthings."Observation"
                FOR ALL
                TO %s
                USING (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_obs_manager_featuresffointerest_insert
                ON sensorthings."FeaturesOfInterest"
                FOR INSERT
                TO %s
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_obs_manager_datastream_update
                ON sensorthings."Datastream"
                FOR UPDATE
                TO %s
                USING (TRUE)
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );

            EXECUTE format(
                'CREATE POLICY %s_obs_manager_location_update
                ON sensorthings."Location"
                FOR UPDATE
                TO %s
                USING (TRUE)
                WITH CHECK (TRUE);',
                policyname_, user_list_
            );
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION sensorthings.remove_user_from_policy(username_ text)
        RETURNS void AS $$
        DECLARE
            old_roles_ text[];
            new_roles_ text[];
            tablename_ text;
            policyname_ text;
        BEGIN
            FOR tablename_, policyname_, old_roles_ IN
                SELECT tablename, policyname, roles
                FROM pg_policies
                WHERE username_ = ANY(roles)
            LOOP
                new_roles_ := array_remove(old_roles_, username_);
                IF new_roles_ = '{}' THEN
                    EXECUTE format('DROP POLICY %I ON sensorthings.%I', policyname_, tablename_);
                ELSE
                    EXECUTE format('ALTER POLICY %I ON sensorthings.%I TO %s', policyname_, tablename_, array_to_string(new_roles_, ','));
                END IF;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION sensorthings.add_users_to_policy(users_ text[], policyname_ text)
        RETURNS TABLE(tablename_ text, cmd_ text) AS $$
        DECLARE
            tablename_ text;
            old_roles_ text[];
            new_roles_ text[];
            cmd_ text;
        BEGIN
            SELECT tablename, roles, cmd INTO tablename_, old_roles_, cmd_
            FROM pg_policies
            WHERE policyname = policyname_;

            IF tablename_ IS NULL THEN
                RAISE EXCEPTION USING ERRCODE = '42704';
            END IF;
            
            new_roles_ := old_roles_ || users_;
            
            EXECUTE format('ALTER POLICY %I ON sensorthings.%I TO %s', policyname_, tablename_, array_to_string(new_roles_, ','));

            RETURN QUERY SELECT tablename_, cmd_; 
        END;
        $$ LANGUAGE plpgsql;

        RESET ROLE;
    END IF;
END $BODY$;