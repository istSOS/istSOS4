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
    commit_id INTEGER;
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
            IF (NEW."phenomenonTime" IS DISTINCT FROM OLD."phenomenonTime") OR IF (NEW."observedArea" IS DISTINCT FROM OLD."observedArea") THEN
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
        OLD."systemTimeValidity" := tstzrange(lower(OLD."systemTimeValidity"), timestamp_now);
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
    -- EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commiter text DEFAULT NULL;', schemaname, tablename);
    -- EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commit_message text DEFAULT NULL;', schemaname, tablename);

    -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I WITH NO DATA;', schemaname || '_history', tablename, schemaname, tablename);
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

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Location_traveltime") RETURNS text AS $$
                SELECT ''/Locations('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
                SELECT ''/Locations('' || $1.id || '')/Things'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
                SELECT ''/Locations('' || $1.id || '')/HistoricalLocations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Locations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
                SELECT ''/Things('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
                SELECT ''/Things('' || $1.id || '')/Locations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
                SELECT ''/Things('' || $1.id || '')/HistoricalLocations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
                SELECT ''/Things('' || $1.id || '')/Datastreams'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Things('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
                SELECT ''/HistoricalLocations('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
                SELECT ''/HistoricalLocations('' || $1.id || '')/Locations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
                SELECT ''/HistoricalLocations('' || $1.id || '')/Thing'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/HistoricalLocations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
        SELECT ''/ObservedProperties('' ||  $1.id || '')'';
    $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
        SELECT ''/ObservedProperties('' || $1.id || '')/Datastreams'';
    $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/ObservedProperties('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
        SELECT ''/Sensors('' || $1.id || '')'';
    $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
        SELECT ''/Sensors('' || $1.id || '')/Datastreams'';
    $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Sensors('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')/Thing'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Sensor@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')/Sensor'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "ObservedProperty@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')/ObservedProperty'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
                SELECT ''/Datastreams('' || $1.id || '')/Observations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Datastreams('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
                SELECT ''/FeaturesOfInterest('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
                SELECT ''/FeaturesOfInterest('' || $1.id || '')/Observations'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/FeaturesOfInterest('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
                SELECT ''/Observations('' || $1.id || '')'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
                SELECT ''/Observations('' || $1.id || '')/FeatureOfInterest'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
                SELECT ''/Observations('' || $1.id || '')/Datastream'';
            $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation_traveltime") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Observations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

    EXECUTE 'CREATE OR REPLACE FUNCTION result(sensorthings."Observation_traveltime") RETURNS jsonb AS $$ 
                BEGIN 
                    RETURN CASE  
                        WHEN $1."resultType" = 0 THEN to_jsonb($1."resultString") 
                        WHEN $1."resultType" = 1 THEN to_jsonb($1."resultInteger") 
                        WHEN $1."resultType" = 2 THEN to_jsonb($1."resultDouble") 
                        WHEN $1."resultType" = 3 THEN to_jsonb($1."resultBoolean") 
                        WHEN $1."resultType" = 4 THEN $1."resultJSON" 
                        ELSE NULL::jsonb 
                    END; 
                END; 
            $$ LANGUAGE plpgsql;';

CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')/Observations';
$$ LANGUAGE SQL;

    RAISE NOTICE 'Schema % is now versionized.', original_schema;
END;
$body$;

DO $body$
BEGIN
    -- Check if custom versioning is enabled
    IF current_setting('custom.versioning', true)::boolean THEN
        -- First, create the Commit table if it doesn't exist
        EXECUTE 'CREATE TABLE IF NOT EXISTS sensorthings."Commit"(
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "author" VARCHAR(255) NOT NULL,
            "encodingType" VARCHAR(100),
            "message" VARCHAR(255) NOT NULL,
            "date" TIMESTAMPTZ DEFAULT NOW()
        );';

        -- Create or replace function for selfLink
        EXECUTE 'CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT ''/Commits('' || $1.id || '')'';
        $$ LANGUAGE SQL;';

        -- Alter the Location table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."Location" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for Location table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_location_commit_id" 
                 ON sensorthings."Location" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for Location table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Locations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the Thing table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."Thing" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for Thing table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_thing_commit_id" 
                 ON sensorthings."Thing" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for Thing table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Things('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the HistoricalLocation table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."HistoricalLocation" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for HistoricalLocation table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_historicallocation_commit_id" 
                 ON sensorthings."HistoricalLocation" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for HistoricalLocation table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/HistoricalLocations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the ObservedProperty table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."ObservedProperty" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for ObservedProperty table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_observedproperty_commit_id" 
                 ON sensorthings."ObservedProperty" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for ObservedProperty table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/ObservedProperties('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the Sensor table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."Sensor" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for Sensor table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_sensor_commit_id" 
                 ON sensorthings."Sensor" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for Sensor table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Sensors('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the Datastream table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."Datastream" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for Datastream table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_datastream_commit_id" 
                 ON sensorthings."Datastream" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for Datastream table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Datastreams('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the FeaturesOfInterest table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."FeaturesOfInterest" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for FeaturesOfInterest table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_featuresofinterest_commit_id" 
                 ON sensorthings."FeaturesOfInterest" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';

        -- Create or replace function for Commit@iot.navigationLink for FeaturesOfInterest table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/FeaturesOfInterest('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Alter the Observation table to add the commit_id column
        EXECUTE 'ALTER TABLE sensorthings."Observation" 
                 ADD COLUMN IF NOT EXISTS "commit_id" BIGINT 
                 REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;';

        -- Create an index on the commit_id column for Observation table
        EXECUTE 'CREATE INDEX IF NOT EXISTS "idx_observation_commit_id" 
                 ON sensorthings."Observation" 
                 USING btree ("commit_id" ASC NULLS LAST) 
                 TABLESPACE pg_default;';
        
        -- Create or replace function for Commit@iot.navigationLink for Observation table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
            SELECT CASE 
                WHEN $1.commit_id IS NOT NULL THEN 
                    ''/Observations('' || $1.id || '')/Commit('' || $1.commit_id || '')''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for Thing@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Thing" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/Thing''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for Location@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Location@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Location" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/Location''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for HistoricalLocation@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "HistoricalLocation@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."HistoricalLocation" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/HistoricalLocation''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for ObservedProperty@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "ObservedProperty@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."ObservedProperty" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/ObservedProperty''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for Sensor@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Sensor@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Sensor" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/Sensor''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for Datastream@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."Datastream" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/Datastream''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for FeatureOfInterest@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sensorthings."FeaturesOfInterest" 
                    WHERE commit_id = $1.id
                ) THEN 
                    ''/Commits('' || $1.id || '')/FeatureOfInterest''
                ELSE 
                    NULL
            END;
        $$ LANGUAGE SQL;';

        -- Create or replace function for Observations@iot.navigationLink in Commit table
        EXECUTE 'CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
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
        $$ LANGUAGE SQL;';

        -- Finally, set up schema versioning
        EXECUTE 'SELECT sensorthings.add_schema_to_versioning(''sensorthings'');';
    END IF;
END $body$;
