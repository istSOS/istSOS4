--CREATE EXTENSION IF NOT exists pg_graphql;
CREATE EXTENSION IF NOT exists postgis;
CREATE EXTENSION IF NOT exists unit;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS btree_gist;
--CREATE EXTENSION IF NOT exists uri;

CREATE SCHEMA sensorthings;

CREATE TABLE IF NOT EXISTS sensorthings."Location" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "location" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb,
    "@iot.selfLink" TEXT,
    "Things@iot.navigationLink" TEXT,
    "HistoricalLocations@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."Thing" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb,
    "location_id" BIGINT REFERENCES sensorthings."Location" (id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Locations@iot.navigationLink" TEXT,
    "HistoricalLocations@iot.navigationLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."HistoricalLocation" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "time" TIMESTAMPTZ NOT NULL,
    "thing_id" BIGINT REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "location_id" BIGINT REFERENCES sensorthings."Location"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Locations@iot.navigationLink" TEXT,
    "Thing@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."ObservedProperty" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "definition" VARCHAR(255) NOT NULL,
    "description" VARCHAR(255) NOT NULL,
    "properties" jsonb,
    "@iot.selfLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" VARCHAR(255) NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" jsonb NOT NULL,
    "properties" jsonb,
    "@iot.selfLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."Datastream" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "unitOfMeasurement" jsonb NOT NULL,
    "observationType" VARCHAR(100) NOT NULL,
    "observedArea" geometry(Polygon, 4326),
    "phenomenonTime" tstzrange,
    "resultTime" tstzrange,
    "properties" jsonb,
    "thing_id" BIGINT REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "sensor_id" BIGINT REFERENCES sensorthings."Sensor"(id) ON DELETE CASCADE,
    "observedproperty_id" BIGINT REFERENCES sensorthings."ObservedProperty"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Thing@iot.navigationLink" TEXT,
    "Sensor@iot.navigationLink" TEXT,
    "ObservedProperty@iot.navigationLink" TEXT,
    "Observations@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."FeaturesOfInterest" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" VARCHAR(255) NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "feature" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb,
    "@iot.selfLink" TEXT,
    "Observations@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."Observation" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "phenomenonTime" TIMESTAMPTZ NOT NULL,
    "resultTime" TIMESTAMPTZ NOT NULL,
    "resultType" INT NOT NULL,
    "resultString" TEXT,
    "resultInteger" INT,
    "resultDouble" DOUBLE PRECISION,
    "resultBoolean" BOOLEAN,
    "resultJSON" jsonb,
    "resultQuality" jsonb,
    "validTime" tstzrange DEFAULT NULL,
    "parameters" jsonb,
    "datastream_id" BIGINT REFERENCES sensorthings."Datastream"(id) ON DELETE CASCADE,
    "featuresofinterest_id" BIGINT REFERENCES sensorthings."FeaturesOfInterest"(id) ON DELETE CASCADE,
    UNIQUE ("datastream_id", "phenomenonTime"),
    "@iot.selfLink" TEXT,
    "FeatureOfInterest@iot.navigationLink" TEXT,
    "Datastream@iot.navigationLink" TEXT
);

-- CREATE OR REPLACE FUNCTION result(sensorthings."Observation") RETURNS text AS $$
--     SELECT CASE WHEN $1."resultType" = 0 THEN $1."resultString"
--                 WHEN $1."resultType" = 1 THEN $1."resultInteger"::text
--                 WHEN $1."resultType" = 2 THEN $1."resultDouble"::text
--                 WHEN $1."resultType" = 3 THEN $1."resultBoolean"::text
--                 WHEN $1."resultType" = 4 THEN $1."resultJSON"::text
--                 ELSE NULL
--              END;
-- $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION update_location_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_location_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Location"
FOR EACH ROW
EXECUTE FUNCTION update_location_self_link();

CREATE OR REPLACE FUNCTION update_location_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Things@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Locations(', NEW.id, ')/Things');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Locations(', NEW.id, ')/HistoricalLocations');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_location_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Location"
FOR EACH ROW
EXECUTE FUNCTION update_location_navigation_links();

CREATE OR REPLACE FUNCTION update_thing_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_thing_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Thing"
FOR EACH ROW
EXECUTE FUNCTION update_thing_self_link();

CREATE OR REPLACE FUNCTION update_thing_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Things(', NEW.id, ')/Locations');
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Things(', NEW.id, ')/Datastreams');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Things(', NEW.id, ')/HistoricalLocations');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_thing_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Thing"
FOR EACH ROW
EXECUTE FUNCTION update_thing_navigation_links();

CREATE OR REPLACE FUNCTION update_historical_location_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_historical_location_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."HistoricalLocation"
FOR EACH ROW
EXECUTE FUNCTION update_historical_location_self_link();

CREATE OR REPLACE FUNCTION update_historical_location_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'HistoricalLocations(', NEW.id, ')/Locations');
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), 'HistoricalLocations(', NEW.id, ')/Thing');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_historical_location_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."HistoricalLocation"
FOR EACH ROW
EXECUTE FUNCTION update_historical_location_navigation_links();

CREATE OR REPLACE FUNCTION update_observed_property_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observed_property_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."ObservedProperty"
FOR EACH ROW
EXECUTE FUNCTION update_observed_property_self_link();

CREATE OR REPLACE FUNCTION update_observed_property_navigation_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), 'ObservedProperties(', NEW.id, ')/Datastreams');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observed_property_navigation_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."ObservedProperty"
FOR EACH ROW
EXECUTE FUNCTION update_observed_property_navigation_link();

CREATE OR REPLACE FUNCTION update_sensor_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sensor_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Sensor"
FOR EACH ROW
EXECUTE FUNCTION update_sensor_self_link();

CREATE OR REPLACE FUNCTION update_sensor_navigation_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Sensors(', NEW.id, ')/Datastreams');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sensor_navigation_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Sensor"
FOR EACH ROW
EXECUTE FUNCTION update_sensor_navigation_link();

CREATE OR REPLACE FUNCTION update_datastream_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_datastream_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Datastream"
FOR EACH ROW
EXECUTE FUNCTION update_datastream_self_link();

CREATE OR REPLACE FUNCTION update_datastream_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Datastreams(', NEW.id, ')/Thing');
    NEW."Sensor@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Datastreams(', NEW.id, ')/Sensor');
    NEW."ObservedProperty@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Datastreams(', NEW.id, ')/ObservedProperty');
    NEW."Observations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Datastreams(', NEW.id, ')/Observations');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_datastream_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Datastream"
FOR EACH ROW
EXECUTE FUNCTION update_datastream_navigation_links();

CREATE OR REPLACE FUNCTION update_feature_of_interest_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_feature_of_interest_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."FeaturesOfInterest"
FOR EACH ROW
EXECUTE FUNCTION update_feature_of_interest_self_link();

CREATE OR REPLACE FUNCTION update_feature_of_interest_navigation_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Observations@iot.navigationLink" := concat(current_setting('custom.hostname'), 'FeaturesOfInterest(', NEW.id, ')/Observations');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_feature_of_interest_navigation_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."FeaturesOfInterest"
FOR EACH ROW
EXECUTE FUNCTION update_feature_of_interest_navigation_link();

CREATE OR REPLACE FUNCTION update_observation_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), TG_TABLE_NAME, '(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observation_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Observation"
FOR EACH ROW
EXECUTE FUNCTION update_observation_self_link();

CREATE OR REPLACE FUNCTION update_observation_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."FeatureOfInterest@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Observations(', NEW.id, ')/FeatureOfInterest');
    NEW."Datastream@iot.navigationLink" := concat(current_setting('custom.hostname'), 'Observations(', NEW.id, ')/Datastream');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observation_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Observation"
FOR EACH ROW
EXECUTE FUNCTION update_observation_navigation_links();

--- =======================
--- SYSTEM_TIME extension
--- =======================

-- triggers to handle table versioning with system_time
CREATE OR REPLACE FUNCTION istsos_mutate_history()
RETURNS trigger 
LANGUAGE plpgsql
AS $body$
-- DECLARE
--     cts TIMESTAMP := current_timestamp;
BEGIN
    IF (TG_OP = 'UPDATE')
    THEN
        -- verify the id is not modified
        IF (NEW.id <> OLD.id)
        THEN
            RAISE EXCEPTION 'the ID must not be changed (%)', NEW.id;
        END IF;
        -- Set the new START system_type_validity for the main table
        NEW.system_time_validity := tstzrange(current_timestamp, TIMESTAMPTZ  'infinity');
        -- Set the END system_time_validity to the 'current_timestamp'
        OLD.system_time_validity := tstzrange(lower(OLD.system_time_validity), current_timestamp);
        -- Copy the original row to the history table
        RAISE NOTICE 'Generated SQL: INSERT INTO %.% VALUES %', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME, OLD;
        -- EXECUTE format('INSERT INTO %I.%I VALUES %s', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME, OLD);
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME) USING OLD;
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'INSERT')
    THEN
        -- Set the new START system_type_validity for the main table
        NEW.system_time_validity := tstzrange(current_timestamp, 'infinity');
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'DELETE')
    THEN
        -- Set the END system_time_validity to the 'current_timestamp'
        OLD.system_time_validity := tstzrange(lower(OLD.system_time_validity), current_timestamp);
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
    -- Quote the schemaname and tablename parameters
    --schemaname := quote_ident(schemaname);
    --tablename := quote_ident(tablename);

    -- Add the new columns for versioning to the original table
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_time_validity tstzrange DEFAULT tstzrange(current_timestamp, TIMESTAMPTZ ''infinity'');', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commiter text DEFAULT NULL;', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commit_message text DEFAULT NULL;', schemaname, tablename);
   
 -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I WITH NO DATA;', schemaname || '_history', tablename, schemaname, tablename);
    -- Add constraint to enforce a single observation does not have two values at the same time
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, system_time_validity WITH &&);', schemaname || '_history', tablename, tablename || '_history_unique_obs');

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

    RAISE NOTICE 'Schema % is now versionized.', original_schema;
END;
$body$;

DO $$
BEGIN
-- Check if custom versioning is enabled
    IF current_setting('custom.versioning', true)::boolean THEN
    -- First, set up schema versioning
        EXECUTE 'SELECT sensorthings.add_schema_to_versioning(''sensorthings'');';

        -- After versioning is set up and Observation_traveltime exists, define the result function
        CREATE OR REPLACE FUNCTION result(sensorthings."Observation_traveltime")
            RETURNS text AS $body$
                SELECT CASE 
                WHEN $1."resultType" = 0 THEN to_json($1."resultString")
                WHEN $1."resultType" = 1 THEN to_json($1."resultInteger")
                WHEN $1."resultType" = 2 THEN to_json($1."resultDouble")
                WHEN $1."resultType" = 3 THEN to_json($1."resultBoolean")
                WHEN $1."resultType" = 4 THEN $1."resultJSON"::json
                ELSE NULL
             END;
            $body$ LANGUAGE SQL;
    END IF;
END $$;
-- ==================
-- STA functions
-- ==================

-- return reference to the entity id
--CREATE OR REPLACE FUNTION sensorthings.refid(uri text, elemntname text)


--elemntname || "@iot.navigationLink" : "Things(1)/Locations",
--"HistoricalLocations@iot.navigationLink" : "Things(1)/HistoricalLocations",
--"Datastreams@iot.navigationLink" : "Things(1)/Datastreams",
--"@iot.id" : 1,
--"@iot.selfLink" : "/SensorThingsService/v1.0/Things(1)"