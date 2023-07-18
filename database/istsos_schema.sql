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
    "location" geometry(geometry, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS sensorthings."Thing" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb,
    "location_id" BIGINT REFERENCES sensorthings."Location" (id)
);


CREATE TABLE IF NOT EXISTS sensorthings."HistoricalLocation" (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    thing_id BIGINT REFERENCES sensorthings."Thing"(id),
    location_id BIGINT REFERENCES sensorthings."Location"(id)
);

CREATE TABLE IF NOT EXISTS sensorthings."ObservedProperty" (
    "id" BIGSERIAL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    --"definition" URI NOT NULL,
    "definition" VARCHAR(255) NOT NULL,
    "description" VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS sensorthings."Datastream" (
    "id" BIGSERIAL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "unitOfMeasurement" jsonb NOT NULL,
    "observationType" VARCHAR(100) NOT NULL,
    "observedArea" geometry(Polygon, 4326),
    "phenomenonTime" tstzrange,
    "resultTime" tstzrange,
    "thing_id" BIGINT REFERENCES sensorthings."Thing"(id) NOT NULL,
    "sensor_id" BIGINT REFERENCES sensorthings."Sensor"(id) NOT NULL,
    "observedproperty_id" BIGINT REFERENCES sensorthings."ObservedProperty"(id)
);


CREATE TABLE IF NOT EXISTS sensorthings."FeaturesOfInterest" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "feature" geometry(geometry, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS sensorthings."Observation" (
    "id" BIGSERIAL PRIMARY KEY,
    "phenomenonTime" TIMESTAMPTZ NOT NULL,
    "resultTime" TIMESTAMPTZ NOT NULL,
    "result" FLOAT NOT NULL,
    "resultQuality" TEXT,
    "validTime" tstzrange DEFAULT NULL,
    "parameters" jsonb,
    "datastream_id" BIGINT REFERENCES sensorthings."Datastream"(id),
    "feature_of_interest_id" BIGINT REFERENCES sensorthings."FeaturesOfInterest"(id),
    UNIQUE ("datastream_id", "phenomenonTime")
);

CREATE OR REPLACE FUNCTION "@iot.id"(anyelement) RETURNS text AS $$
  SELECT $1.id;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(anyelement) RETURNS text AS $$
  SELECT concat(current_setting('custom.hostname'),substring(pg_typeof($1)::text from 2 for length(pg_typeof($1)::text) - 2),'(' || $1.id || ')');
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."Thing") RETURNS table(
    "Locations@iot.navigationLink" text,
    "Datastreams@iot.navigationLink" text,
    "HistoricalLocations@iot.navigationLink" text
) AS 
$$
  SELECT concat(current_setting('custom.hostname'),'Things(' || $1.id || ')/Locations'),
         concat(current_setting('custom.hostname'),'Things(' || $1.id || ')/Datastreams'),
         concat(current_setting('custom.hostname'),'Things(' || $1.id || ')/HistoricalLocations');
$$ 
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."Location") RETURNS table(
    "Things@iot.navigationLink" text,
    "HistoricalLocations@iot.navigationLink" text
) AS 
$$
  SELECT concat(current_setting('custom.hostname'),'Locations(' || $1.id || ')/Things'),
         concat(current_setting('custom.hostname'),'Locations(' || $1.id || ')/HistoricalLocations');
$$ 
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS table(
    "Locations@iot.navigationLink" text,
    "Thing@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'HistoricalLocations(' || $1.id || ')/Locations'),
         concat(current_setting('custom.hostname'),'HistoricalLocations(' || $1.id || ')/Thing');
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."Datastream") RETURNS table(
    "Thing@iot.navigationLink" text,
    "Sensor@iot.navigationLink" text,
    "ObservedProperty@iot.navigationLink" text,
    "Observations@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'Datastreams(' || $1.id || ')/Thing'),
         concat(current_setting('custom.hostname'),'Datastreams(' || $1.id || ')/Sensor'),
         concat(current_setting('custom.hostname'),'Datastreams(' || $1.id || ')/ObservedProperty'),
         concat(current_setting('custom.hostname'),'Datastreams(' || $1.id || ')/Observations');
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."Observation") RETURNS table(
    "FeatureOfInterest@iot.navigationLink" text,
    "Datastream@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'Observations(' || $1.id || ')/FeatureOfInterest'),
         concat(current_setting('custom.hostname'),'Observations(' || $1.id || ')/Datastream');
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS table(
    "Observations@iot.navigationLink" text,
    "skip@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'FeaturesOfInterest(' || $1.id || ')/Observations'), 'skip';
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."Sensor") RETURNS table(
    "Datastreams@iot.navigationLink" text,
    "skip@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'Sensors(' || $1.id || ')/Datastreams'), 'skip';
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS table(
    "Datastreams@iot.navigationLink" text,
    "skip@iot.navigationLink" text
) AS
$$
  SELECT concat(current_setting('custom.hostname'),'ObservedProperties(' || $1.id || ')/Datastreams'), 'skip';
$$
LANGUAGE SQL;

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
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA, TG_TABLE_NAME || '_history') USING OLD;
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF TG_OP IN ('INSERT')
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
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA, TG_TABLE_NAME || '_history') USING OLD;
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

    -- Add the new columnscolumn for versioning to the original table
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_time_validity tstzrange DEFAULT tstzrange(current_timestamp, TIMESTAMPTZ ''infinity'');', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commiter text DEFAULT NULL;', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commit_message text DEFAULT NULL;', schemaname, tablename);

    -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I WITH NO DATA;', schemaname, tablename || '_history', schemaname, tablename);

    -- Add constrain to enforce a single observation does not have two values at the same time
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, system_time_validity WITH &&);', schemaname, tablename || '_history', tablename || '_history_unique_obs');

    -- Add triggers for versioning
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE istsos_mutate_history();', tablename || '_history_trigger', schemaname, tablename, schemaname);

    -- Add triggers to rise error if history table is updated or deleted
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION istsos_prevent_table_update();', tablename || '_history_no_mutate', schemaname, tablename || '_history', schemaname);

    -- Create the travelitime view to query data modification history
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I UNION SELECT * FROM %I.%I;',
        schemaname, tablename || '_traveltime',
        schemaname, tablename,
        schemaname, tablename || '_history');

    RAISE NOTICE '%s.%s is now added to versioning', schemaname, tablename;
END;
$body$;

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
