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
    "description" TEXT NOT NULL,
    "properties" jsonb,
    "@iot.selfLink" TEXT,
    "Datastreams@iot.navigationLink" TEXT
);

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) UNIQUE NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" VARCHAR(255) NOT NULL,
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
    "description" TEXT NOT NULL,
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

CREATE OR REPLACE FUNCTION location_geojson(sensorthings."Location")
RETURNS jsonb AS $$
DECLARE
    geojson jsonb;
BEGIN
    geojson := jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON($1."location")::jsonb
    );
    RETURN geojson;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION observed_area_geojson(sensorthings."Datastream")
RETURNS jsonb AS $$
BEGIN
    RETURN ST_AsGeoJSON($1."observedArea")::jsonb;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION feature_geojson(sensorthings."FeaturesOfInterest")
RETURNS jsonb AS $$
DECLARE
    geojson jsonb;
BEGIN
    geojson := jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON($1."feature")::jsonb
    );
    RETURN geojson;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_location_self_link()
RETURNS TRIGGER AS $$
BEGIN
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Things@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Locations(', NEW.id, ')/Things');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Locations(', NEW.id, ')/HistoricalLocations');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Things(', NEW.id, ')/Locations');
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Things(', NEW.id, ')/Datastreams');
    NEW."HistoricalLocations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Things(', NEW.id, ')/HistoricalLocations');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Locations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/HistoricalLocations(', NEW.id, ')/Locations');
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/HistoricalLocations(', NEW.id, ')/Thing');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Datastreams@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Sensors(', NEW.id, ')/Datastreams');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."Thing@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Datastreams(', NEW.id, ')/Thing');
    NEW."Sensor@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Datastreams(', NEW.id, ')/Sensor');
    NEW."ObservedProperty@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Datastreams(', NEW.id, ')/ObservedProperty');
    NEW."Observations@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Datastreams(', NEW.id, ')/Observations');
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
    NEW."@iot.selfLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/', TG_TABLE_NAME, '(', NEW.id, ')');
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
    NEW."FeatureOfInterest@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Observations(', NEW.id, ')/FeatureOfInterest');
    NEW."Datastream@iot.navigationLink" := concat(current_setting('custom.hostname'), current_setting('custom.subpath'), current_setting('custom.version'), '/Observations(', NEW.id, ')/Datastream');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_observation_navigation_links_trigger
BEFORE INSERT OR UPDATE ON sensorthings."Observation"
FOR EACH ROW
EXECUTE FUNCTION update_observation_navigation_links();