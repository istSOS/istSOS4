CREATE EXTENSION IF NOT exists postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA sensorthings;

CREATE TABLE IF NOT EXISTS sensorthings."Commit"(
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "author" VARCHAR(255) NOT NULL,
    "encodingType" VARCHAR(100),
    "message" VARCHAR(255) NOT NULL,
    "date" TIMESTAMPTZ DEFAULT NOW(),
    "@iot.selfLink" TEXT,
    "Thing@iot.navigationLink" TEXT,
    "Location@iot.navigationLink" TEXT,
    "HistoricalLocation@iot.navigationLink" TEXT,
    "ObservedProperty@iot.navigationLink" TEXT,
    "Sensor@iot.navigationLink" TEXT,
    "Datastream@iot.navigationLink" TEXT,
    "FeatureOfInterest@iot.navigationLink" TEXT,
    "Observation@iot.navigationLink" TEXT
);

CREATE INDEX "idx_commit_id" ON sensorthings."Commit" USING btree ("id");

CREATE TABLE IF NOT EXISTS sensorthings."Location" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "location" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Things@iot.navigationLink" TEXT,
    "HistoricalLocations@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT,
    "gen_foi_id" BIGINT
);

CREATE UNIQUE INDEX "idx_location_id" ON sensorthings."Location" USING btree ("id");
CREATE INDEX "idx_location_commit_id" ON sensorthings."Location" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Thing" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Locations@iot.navigationLink" TEXT,
    "HistoricalLocations@iot.navigationLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_thing_id" ON sensorthings."Thing" USING btree ("id");
CREATE INDEX "idx_thing_commit_id" ON sensorthings."Thing" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Thing_Location" (
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "location_id" BIGINT NOT NULL REFERENCES sensorthings."Location"(id) ON DELETE CASCADE,
    CONSTRAINT thing_location_unique UNIQUE ("thing_id", "location_id")
);

CREATE UNIQUE INDEX "idx_thing_location_id" ON sensorthings."Thing_Location" USING btree ("thing_id", "location_id");
CREATE INDEX "idx_thing_location_thing_id" ON sensorthings."Thing_Location" USING btree ("thing_id");
CREATE INDEX "idx_thing_location_location_id" ON sensorthings."Thing_Location" USING btree ("location_id");

CREATE TABLE IF NOT EXISTS sensorthings."HistoricalLocation" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "time" TIMESTAMPTZ DEFAULT NOW(),
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Locations@iot.navigationLink" TEXT,
    "Thing@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_historicallocation_id" ON sensorthings."HistoricalLocation" USING btree ("id");
CREATE INDEX "idx_historicallocation_thing_id" ON sensorthings."HistoricalLocation" USING btree ("thing_id");
CREATE INDEX "idx_historicallocation_commit_id" ON sensorthings."HistoricalLocation" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Location_HistoricalLocation" (
    "location_id" BIGINT NOT NULL REFERENCES sensorthings."Location"(id) ON DELETE CASCADE,
    "historicallocation_id" BIGINT NOT NULL REFERENCES sensorthings."HistoricalLocation"(id) ON DELETE CASCADE,
    CONSTRAINT location_historical_location_unique UNIQUE ("location_id", "historicallocation_id")
);

CREATE UNIQUE INDEX "idx_location_historicallocation_id" ON sensorthings."Location_HistoricalLocation" USING btree ("location_id", "historicallocation_id");
CREATE INDEX "idx_location_historicallocation_location_id" ON sensorthings."Location_HistoricalLocation" USING btree ("location_id");
CREATE INDEX "idx_location_historicallocation_historicallocation_id" ON sensorthings."Location_HistoricalLocation" USING btree ("historicallocation_id");

CREATE TABLE IF NOT EXISTS sensorthings."ObservedProperty" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "definition" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_observedproperty_id" ON sensorthings."ObservedProperty" USING btree ("id");
CREATE INDEX "idx_observedproperty_commit_id" ON sensorthings."ObservedProperty" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" VARCHAR(255) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_sensor_id" ON sensorthings."Sensor" USING btree ("id");
CREATE INDEX "idx_sensor_commit_id" ON sensorthings."Sensor" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Datastream" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "unitOfMeasurement" jsonb NOT NULL,
    "observationType" VARCHAR(100) NOT NULL,
    "observedArea" geometry(Polygon, 4326),
    "phenomenonTime" tstzrange,
    "resultTime" tstzrange,
    "properties" jsonb DEFAULT NULL,
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "sensor_id" BIGINT NOT NULL REFERENCES sensorthings."Sensor"(id) ON DELETE CASCADE,
    "observedproperty_id" BIGINT NOT NULL REFERENCES sensorthings."ObservedProperty"(id) ON DELETE CASCADE,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Thing@iot.navigationLink" TEXT,
    "Sensor@iot.navigationLink" TEXT,
    "ObservedProperty@iot.navigationLink" TEXT,
    "Observations@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_datastream_id" ON sensorthings."Datastream" USING btree ("id");
CREATE INDEX "idx_datastream_thing_id" ON sensorthings."Datastream" USING btree ("thing_id");
CREATE INDEX "idx_datastream_sensor_id" ON sensorthings."Datastream" USING btree ("sensor_id");
CREATE INDEX "idx_datastream_observedproperty_id" ON sensorthings."Datastream" USING btree ("observedproperty_id");
CREATE INDEX "idx_datastream_commit_id" ON sensorthings."Datastream" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."FeaturesOfInterest" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "feature" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "Observations@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_featuresofinterest_id" ON sensorthings."FeaturesOfInterest" USING btree ("id");
CREATE INDEX "idx_featuresofinterest_commit_id" ON sensorthings."FeaturesOfInterest" USING btree ("commit_id");

CREATE TABLE IF NOT EXISTS sensorthings."Observation" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "phenomenonTime" TIMESTAMPTZ DEFAULT NOW(),
    "resultTime" TIMESTAMPTZ DEFAULT NULL,
    "resultType" INT NOT NULL,
    "resultString" TEXT,
    "resultInteger" INT,
    "resultDouble" DOUBLE PRECISION,
    "resultBoolean" BOOLEAN,
    "resultJSON" jsonb,
    "resultQuality" jsonb DEFAULT NULL,
    "validTime" tstzrange DEFAULT NULL,
    "parameters" jsonb DEFAULT NULL,
    "datastream_id" BIGINT NOT NULL REFERENCES sensorthings."Datastream"(id) ON DELETE CASCADE,
    "featuresofinterest_id" BIGINT NOT NULL REFERENCES sensorthings."FeaturesOfInterest"(id) ON DELETE CASCADE,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "@iot.selfLink" TEXT,
    "FeatureOfInterest@iot.navigationLink" TEXT,
    "Datastream@iot.navigationLink" TEXT,
    "Commit@iot.navigationLink" TEXT
);

CREATE UNIQUE INDEX "idx_observation_id" ON sensorthings."Observation" USING btree ("id");
CREATE INDEX "idx_observation_datastream_id" ON sensorthings."Observation" USING btree ("datastream_id");
CREATE INDEX "idx_observation_featuresofinterest_id" ON sensorthings."Observation" USING btree ("featuresofinterest_id");
CREATE INDEX "idx_observation_observation_id_datastream_id" ON sensorthings."Observation" USING btree ("id", "datastream_id");
CREATE INDEX "idx_observation_commit_id" ON sensorthings."Observation" USING btree ("commit_id");

CREATE OR REPLACE FUNCTION result(sensorthings."Observation") RETURNS jsonb AS $$
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
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_commit_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_commit_self_link_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Commit"
FOR EACH ROW
EXECUTE FUNCTION update_commit_self_link();

CREATE OR REPLACE FUNCTION update_commit_navigation_links()
RETURNS TRIGGER AS $$
BEGIN
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Thing');
    NEW."Location@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Location');
    NEW."HistoricalLocation@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/HistoricalLocation');
    NEW."ObservedProperty@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/ObservedProperty');
    NEW."Sensor@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Sensor');
    NEW."Datastream@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Datastream');
    NEW."FeatureOfInterest@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/FeatureOfInterest');
    NEW."Observation@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Observation');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_commit_navigation_links_trigger
BEFORE INSERT ON sensorthings."Commit"
FOR EACH ROW
EXECUTE FUNCTION update_commit_navigation_links();

CREATE OR REPLACE FUNCTION update_location_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."Things@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Things');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/HistoricalLocations');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Locations');
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Datastreams');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/HistoricalLocations');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Locations');
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Thing');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/ObservedProperties(', NEW.id, ')');
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
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/ObservedProperties(', NEW.id, ')/Datastreams');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/ObservedProperties(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Datastreams');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Thing');
    NEW."Sensor@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Sensor');
    NEW."ObservedProperty@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/ObservedProperty');
    NEW."Observations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Observations');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Observations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/FeaturesOfInterest(', NEW.id, ')/Observations');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/FeaturesOfInterest(', NEW.id, ')/Commit');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')');
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
    NEW."FeatureOfInterest@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/FeatureOfInterest');
    NEW."Datastream@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Datastream');
    NEW."Commit@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, 's(', NEW.id, ')/Commit');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observation_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Observation"
FOR EACH ROW
EXECUTE FUNCTION update_observation_navigation_links();

-- Create the trigger function
CREATE OR REPLACE FUNCTION delete_related_historical_locations() RETURNS TRIGGER AS $$
BEGIN
    -- Delete HistoricalLocations where the thing_id matches the deleted Location's thing_id
    DELETE FROM sensorthings."HistoricalLocation"
    WHERE thing_id = (
        SELECT tl.thing_id
        FROM sensorthings."Thing_Location" tl
        WHERE tl.location_id = OLD.id
    );

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER before_location_delete
BEFORE DELETE ON sensorthings."Location"
FOR EACH ROW
EXECUTE FUNCTION delete_related_historical_locations();

create or replace function sensorthings.count_estimate(query text)
  returns integer
  language plpgsql as
$func$
declare
    rec record;

rows integer;

begin
    for rec in execute 'EXPLAIN ' || query loop
        rows := substring(rec."QUERY PLAN"
from
' rows=([[:digit:]]+)');

exit
when rows is not null;
end loop;

return rows;
end
$func$;