-- =======================
-- SYSTEM_TIME extension
-- =======================

-- triggers to handle table versioning with system_time
CREATE OR REPLACE FUNCTION istsos_mutate_history()
RETURNS trigger 
LANGUAGE plpgsql
AS $body$
DECLARE
    timestamp_now TIMESTAMPTZ := current_timestamp;
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        -- Verify the id is not modified
        IF (NEW.id <> OLD.id) THEN
            RAISE EXCEPTION 'the ID must not be changed (%)', NEW.id;
        END IF;

        -- If the table is 'Location' and the column 'gen_foi_id' exists and is updated
        IF TG_TABLE_NAME = 'Location' THEN
            IF (NEW.gen_foi_id IS DISTINCT FROM OLD.gen_foi_id AND NEW.gen_foi_id IS NOT NULL) THEN
                -- Skip systemTimeValidity update
                RETURN NEW;
            END IF;
        END IF;

        -- If the table is 'Datastream' and the column 'phenomenonTime' or columns 'observedArea' exist  and are updated
        IF TG_TABLE_NAME = 'Datastream' THEN
            IF (NEW."phenomenonTime" IS DISTINCT FROM OLD."phenomenonTime") OR (NEW."observedArea" IS DISTINCT FROM OLD."observedArea") THEN
                -- Skip systemTimeValidity update
                RETURN NEW;
            END IF;
        END IF;
        -- Set the new START systemTimeValidity for the main table
        NEW."systemTimeValidity" := tstzrange(timestamp_now, 'infinity');
        -- Set the END systemTimeValidity to the 'timestamp_now'
        OLD."systemTimeValidity" := tstzrange(lower(OLD."systemTimeValidity"), timestamp_now);
        -- Copy the original row to the history table
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME) USING OLD;

        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'INSERT') THEN
        -- Set the new START systemTimeValidity for the main table
        NEW."systemTimeValidity" := tstzrange(timestamp_now, TIMESTAMPTZ 'infinity');
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'DELETE') THEN
        -- Set the END systemTimeValidity to the 'timestamp_now'
        OLD."systemTimeValidity" := tstzrange(lower(OLD."systemTimeValidity"), timestamp_now, '[]');
        -- Copy the original row to the history table
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME) USING OLD;
        RETURN OLD;
    END IF;
END;
$body$;


CREATE OR REPLACE FUNCTION istsos_prevent_table_update()
RETURNS trigger 
LANGUAGE plpgsql
AS $body$
BEGIN
    RAISE EXCEPTION 'Updates or Deletes on this table are not allowed';
    RETURN NULL;
END;
$body$;

-- function to add a table to system_time versioning system
CREATE OR REPLACE FUNCTION sensorthings.add_table_to_versioning(tablename text, schemaname text DEFAULT 'public')
RETURNS void 
LANGUAGE plpgsql
AS $body$
BEGIN
    -- Add the new columns for versioning to the original table
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN "systemTimeValidity" tstzrange DEFAULT tstzrange(current_timestamp, TIMESTAMPTZ ''infinity'');', schemaname, tablename);

    -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I WITH NO DATA;', schemaname || '_history', tablename, schemaname, tablename);

    -- EXECUTE format('ALTER TABLE %I.%I ADD COLUMN "deleted_commit_id" BIGINT DEFAULT NULL;', schemaname || '_history', tablename);

    -- Add constraint to enforce a single observation does not have two values at the same time
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, "systemTimeValidity" WITH &&);', schemaname || '_history', tablename, tablename || '_history_unique_obs');

    -- Add triggers for versioning
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE istsos_mutate_history();', tablename || '_history_trigger', schemaname, tablename);

    -- Add triggers to raise an error if the history table is updated or deleted
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION istsos_prevent_table_update();', tablename || '_history_no_mutate', schemaname || '_history', tablename);

    -- Create the travelitime view to query data modification history
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I UNION SELECT * FROM %I.%I;',
        schemaname, tablename || '_traveltime',
        schemaname, tablename,
        schemaname || '_history', tablename);

    RAISE NOTICE '%.% is now added to versioning', schemaname, tablename;
END;
$body$;

CREATE OR REPLACE FUNCTION sensorthings.add_schema_to_versioning(original_schema text)
RETURNS void
LANGUAGE plpgsql
AS $body$
DECLARE
    tablename text;
BEGIN
    -- Create the history schema if it doesn't exist
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I_history;', original_schema);

    -- Loop through each table in the original schema in the correct order
    FOR tablename IN
        SELECT unnest(array['Location', 'Thing', 'HistoricalLocation', 'ObservedProperty', 'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation'])
        LOOP
            -- Add versioning to each table
            EXECUTE format('SELECT sensorthings.add_table_to_versioning(%L, %L);', tablename, original_schema);
        END LOOP;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Location_traveltime") RETURNS text AS $$
        SELECT '/Locations(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
        SELECT '/Locations(' || $1.id || ')/Things';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
        SELECT '/Locations(' || $1.id || ')/HistoricalLocations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
        SELECT '/Locations(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
        SELECT '/Things(' || $1.id ||')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
        SELECT '/Things(' || $1.id || ')/Locations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
        SELECT '/Things(' || $1.id || ')/HistoricalLocations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
        SELECT '/Things(' || $1.id || ')/Datastreams';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
        SELECT '/Things(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
        SELECT '/HistoricalLocations(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
        SELECT '/HistoricalLocations(' || $1.id || ')/Locations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
        SELECT '/HistoricalLocations(' || $1.id || ')/Thing';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
        SELECT '/HistoricalLocations(' || $1.id || ')/Commit(' || $1.commit_id || ')'
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
        SELECT '/ObservedProperties(' ||  $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
        SELECT '/ObservedProperties(' || $1.id || ')/Datastreams';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
        SELECT '/ObservedProperties(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
        SELECT '/Sensors(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
        SELECT '/Sensors(' || $1.id || ')/Datastreams';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
        SELECT '/Sensors(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/Thing';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Sensor@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/Sensor';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "ObservedProperty@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/ObservedProperty';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/Observations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
        SELECT '/FeaturesOfInterest(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
        SELECT '/FeaturesOfInterest(' || $1.id || ')/Observations';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
        SELECT '/FeaturesOfInterest(' || $1.id || ')/Commit(' || $1.commit_id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')/FeatureOfInterest';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')/Datastream';
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
        SELECT CASE 
            WHEN $1.commit_id IS NOT NULL THEN 
                '/Observations(' || $1.id || ')/Commit(' || $1.commit_id || ')'
            ELSE 
                NULL
        END;
    $$ LANGUAGE SQL;

    CREATE OR REPLACE FUNCTION result(sensorthings."Observation_traveltime") RETURNS jsonb AS $$ 
        BEGIN 
            RETURN CASE  
                WHEN $1."resultType" = 0 THEN to_jsonb($1."resultNumber") 
                WHEN $1."resultType" = 1 THEN to_jsonb($1."resultBoolean") 
                WHEN $1."resultType" = 2 THEN $1."resultJSON"  
                WHEN $1."resultType" = 3 THEN to_jsonb($1."resultString")
                ELSE NULL::jsonb 
            END; 
        END; 
    $$ LANGUAGE plpgsql;


    RAISE NOTICE 'Schema % is now versionized.', original_schema;
END;
$body$;

DO $body$
BEGIN
    -- Check if custom versioning is enabled
    IF current_setting('custom.versioning')::boolean THEN
        -- First, create the Commit table if it doesn't exist
        CREATE TABLE IF NOT EXISTS sensorthings."Commit"(
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "author" VARCHAR(255) NOT NULL,
            "encodingType" VARCHAR(100),
            "message" VARCHAR(255) NOT NULL,
            "date" TIMESTAMPTZ DEFAULT NOW(),
            "actionType" VARCHAR(100) NOT NULL CHECK ("actionType" IN ('INSERT', 'UPDATE', 'DELETE')),
            "user_id" BIGINT REFERENCES sensorthings."User"(id) ON DELETE CASCADE
        );

        -- Create or replace function for selfLink
        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT '/Commits(' || $1.id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
            SELECT '/Locations(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
            SELECT '/Things(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
            SELECT '/HistoricalLocations(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS text AS $$
            SELECT '/ObservedProperties(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor") RETURNS text AS $$
            SELECT '/Sensors(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

        -- Alter the Datastream table to add the commit_id column
        ALTER TABLE sensorthings."Datastream" 
        ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
        REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

        -- Create an index on the commit_id column for Datastream table
        CREATE INDEX IF NOT EXISTS "idx_datastream_commit_id" 
        ON sensorthings."Datastream" 
        USING btree ("commit_id" ASC NULLS LAST) 
        TABLESPACE pg_default;

        -- Create or replace function for Commit@iot.navigationLink for Datastream table
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
            SELECT '/Datastreams(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
            SELECT '/FeaturesOfInterest(' || $1.id || ')/Commit(' || $1.commit_id || ')';
        $$ LANGUAGE SQL;

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
        CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    '/Observations(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for Things@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Thing" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/Things'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for Locations@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Location" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/Locations'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for HistoricalLocations@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."HistoricalLocation" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/HistoricalLocations'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for ObservedProperties@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "ObservedProperties@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."ObservedProperty" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/ObservedProperties'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for Sensors@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "Sensors@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Sensor" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/Sensors'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for Datastreams@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Datastream" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/Datastreams'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for FeaturesOfInterest@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "FeaturesOfInterest@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."FeaturesOfInterest" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/FeaturesOfInterest'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Create or replace function for Observations@iot.navigationLink in Commit table
        CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Observation" 
                    WHERE commit_id = $1.id
                ) THEN 
                    '/Commits(' || $1.id || ')/Observations'
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;

        -- Finally, set up schema versioning
        EXECUTE 'SELECT sensorthings.add_schema_to_versioning(''sensorthings'');';

        IF current_setting('custom.authorization')::boolean THEN
            -- Grant permissions to the roles on Commit table
            GRANT ALL PRIVILEGES ON TABLE sensorthings."Commit" TO sensorthings_admin;
            GRANT ALL PRIVILEGES ON SEQUENCE sensorthings."Commit_id_seq" TO sensorthings_admin;

            GRANT SELECT ON TABLE sensorthings."Commit" TO sensorthings_viewer;
            GRANT SELECT ON SEQUENCE sensorthings."Commit_id_seq" TO sensorthings_viewer;

            GRANT SELECT, INSERT ON TABLE sensorthings."Commit" TO sensorthings_editor;
            GRANT USAGE, SELECT ON SEQUENCE sensorthings."Commit_id_seq" TO sensorthings_editor;

            GRANT SELECT ON TABLE sensorthings."Commit" TO sensorthings_sensor;
            GRANT USAGE, SELECT ON SEQUENCE sensorthings."Commit_id_seq" TO sensorthings_sensor;

            GRANT SELECT, INSERT ON TABLE sensorthings."Commit" TO sensorthings_obs_manager;
            GRANT USAGE, SELECT ON SEQUENCE sensorthings."Commit_id_seq" TO sensorthings_obs_manager;

            -- Grant permissions to the roles on the versioned tables
            GRANT CREATE, USAGE ON SCHEMA sensorthings_history TO sensorthings_admin;
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sensorthings_history TO sensorthings_admin;

            GRANT USAGE ON SCHEMA sensorthings_history TO sensorthings_viewer;
            GRANT SELECT ON ALL TABLES IN SCHEMA sensorthings_history TO sensorthings_viewer;

            GRANT USAGE ON SCHEMA sensorthings_history TO sensorthings_editor;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sensorthings_history TO sensorthings_editor;

            GRANT USAGE ON SCHEMA sensorthings_history TO sensorthings_sensor;
            GRANT INSERT ON TABLE sensorthings_history."Observation" TO sensorthings_sensor;
            GRANT INSERT ON TABLE sensorthings_history."Datastream" TO sensorthings_sensor;

            GRANT USAGE ON SCHEMA sensorthings_history TO sensorthings_obs_manager;
            GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE sensorthings_history."Observation" TO sensorthings_obs_manager;
            GRANT INSERT ON TABLE sensorthings_history."Datastream" TO sensorthings_obs_manager;
        END IF;
    END IF;
END $body$;
