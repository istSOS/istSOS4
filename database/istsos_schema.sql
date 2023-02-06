CREATE EXTENSION pg_graphql;
CREATE EXTENSION postgis;
CREATE EXTENSION unit;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- FUNCTIONS
CREATE OR REPLACE FUNCTION public.check_unit(
	_unit text,
	OUT result boolean)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	ret unit;
 BEGIN

    SELECT _unit::unit INTO ret;
	
    IF NOT FOUND THEN
        RAISE EXCEPTION 'The unit does not exist in the SI database.';
    END IF;

    RESULT := TRUE;
END
$BODY$;

-- FEATURE OF INTEREST
CREATE TABLE IF NOT EXISTS public.feature_of_interest (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "geom" geometry(geometry, 4326),
    "encoding_type" text NOT NULL,
    "properties" jsonb
);
-- OBSERVED PROPERTIES
CREATE TABLE IF NOT EXISTS public.observed_property (
    "id" uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    "name" text NOT NULL,
    "description" text,
    "definition" text UNIQUE NOT NULL,
    "constraints" jsonb
);
-- CONTACT TYPE
DROP TYPE IF EXISTS public.contact_type;
CREATE TYPE public.contact_type AS ENUM(
    'owner',
    'manufacturer',
    'operator'
);
-- CONTACT
CREATE TABLE IF NOT EXISTS public.contact(
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "type" public.contact_type DEFAULT 'owner',
    "person" text,
    "telephone" text,
    "fax" text,
    "email" text,
    "web" text,
    "address" text,
    "city" text,
    "admin_area" text,
    "postal_code" text,
    "country" text
);
-- SENSOR TYPE
CREATE TABLE IF NOT EXISTS public.sensor_type (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "description" text,
    "metadata" text
);
-- SENSOR
CREATE TABLE IF NOT EXISTS public.sensor (
    "id" uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    "name" text UNIQUE NOT NULL,
    "description" text,
    "encoding_type" text NOT NULL,
    "sampling_time_resolution" interval,
    "acquisition_time_resolution" interval,
    "sensor_type_id" BIGINT,
    CONSTRAINT fk_sensor_type FOREIGN KEY("sensor_type_id") REFERENCES public.sensor_type("id")
);
-- SENSOR_CONTACT
CREATE TABLE IF NOT EXISTS public.sensor_contact (
    "sensor_id" uuid REFERENCES public.sensor ("id"),
    "contact_id" BIGINT,
    CONSTRAINT fk_contact FOREIGN KEY("contact_id") REFERENCES public.contact("id"),
    UNIQUE ("sensor_id", "contact_id")
);
-- OFFERING
CREATE TABLE IF NOT EXISTS public.offering (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "expiration_time" timestamp with time zone
);
-- THING
CREATE TABLE IF NOT EXISTS public.thing (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "properties" jsonb,
    "offering_id" BIGINT,
    CONSTRAINT fk_offering FOREIGN KEY("offering_id") REFERENCES public.offering("id")
);
-- LOCATION
CREATE TABLE IF NOT EXISTS public.location (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "geom" geometry(geometry, 4326) NOT NULL,
    "encoding_type" text,
    "location" text,
    "properties" jsonb
);
-- THING_LOCATION
CREATE TABLE IF NOT EXISTS public.thing_location (
    "thing_id" BIGINT,
    "location_id" BIGINT,
    CONSTRAINT fk_thing FOREIGN KEY("thing_id") REFERENCES public.thing("id"),
    CONSTRAINT fk_location FOREIGN KEY("location_id") REFERENCES public.location("id")
);
-- UNIT OF MEASURE
-- CREATE TABLE IF NOT EXISTS public.unit_measure (
--     "id" BIGSERIAL NOT NULL PRIMARY KEY,
--     "unit_name" character varying(255) NOT NULL,
--     "unit_symbol" character varying(255),
--     "unit_definition" character varying(255)
-- );
-- DATASTREAM
CREATE TABLE IF NOT EXISTS public.datastream (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "observed_area" geometry(geometry, 4326) NOT NULL,
    "phenomenon_time_start" timestamp with time zone NOT NULL,
    "phenomenon_time_end" timestamp with time zone NOT NULL,
    "result_time_start" timestamp with time zone,
    "result_time_end" timestamp with time zone,
    "properties" jsonb,
    "constraints" jsonb,
    "sensor_id" uuid REFERENCES public.sensor ("id"),
    "observed_property_id" uuid REFERENCES public.observed_property ("id"),
    "thing_id" BIGINT,
    CONSTRAINT fk_thing FOREIGN KEY("thing_id") REFERENCES public.thing("id"),
    "unit_measure" character varying,
    CONSTRAINT check_unit CHECK ( check_unit(unit_measure) )
    -- "unit_measure_id" BIGINT,
    -- CONSTRAINT fk_unit_measure FOREIGN KEY("unit_measure_id") REFERENCES public.unit_measure("id")
);
-- USER
CREATE TABLE IF NOT EXISTS public.user (
    "id" uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    "username" text NOT NULL UNIQUE,
    "firstname" text,
    "lastname" text,
    "mail" text NOT NULL
);
-- QUALITY
CREATE TABLE IF NOT EXISTS public.quality_index (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text
);
-- OBSERVATION UNIT
CREATE TABLE IF NOT EXISTS public.observation_unit (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" unit,
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_unit', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_unit
  ON public.observation_unit(id, phenomenon_time);

CREATE INDEX ON public.observation_unit (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_unit (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_unit (feature_id, phenomenon_time DESC);
-- OBSERVATION JSON
CREATE TABLE IF NOT EXISTS public.observation_json (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" jsonb,
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_json', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_json
  ON public.observation_json(id, phenomenon_time);

CREATE INDEX ON public.observation_json (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_json (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_json (feature_id, phenomenon_time DESC);
-- OBSERVATION BOOLEAN
CREATE TABLE IF NOT EXISTS public.observation_bool (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" boolean,
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_bool', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_bool
  ON public.observation_bool(id, phenomenon_time);

CREATE INDEX ON public.observation_bool (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_bool (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_bool (feature_id, phenomenon_time DESC);
-- OBSERVATION NUMBER
CREATE TABLE IF NOT EXISTS public.observation_number (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" double precision,
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_number', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_number
  ON public.observation_number(id, phenomenon_time);

CREATE INDEX ON public.observation_number (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_number (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_number (feature_id, phenomenon_time DESC);
-- OBSERVATION STRING
CREATE TABLE IF NOT EXISTS public.observation_string (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" text,
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_string', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_string
  ON public.observation_string(id, phenomenon_time);

CREATE INDEX ON public.observation_string (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_string (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_string (feature_id, phenomenon_time DESC);
-- OBSERVATION ARRAY
CREATE TABLE IF NOT EXISTS public.observation_array (
    "id" BIGSERIAL NOT NULL,
    "phenomenon_time" timestamp with time zone NOT NULL,
    "phenomenon_interval" interval DEFAULT NULL,
    "result_time" timestamp with time zone,
    "result_interval" interval DEFAULT NULL,
    "result" float[],
    "valid_time" timestamp with time zone NOT NULL DEFAULT now(),
    "valid_interval"  interval DEFAULT NULL,
    "parameters" jsonb,
    "result_type" text,
    "expiratione_time" timestamp with time zone,
    "motivation" text,
    "user_id" uuid REFERENCES public.user ("id"),
    "quality_id" BIGINT,
    CONSTRAINT fk_quality_index FOREIGN KEY("quality_id") REFERENCES public.quality_index("id"),
    "datastream_id" BIGINT,
    CONSTRAINT fk_datastream FOREIGN KEY("datastream_id") REFERENCES public.datastream("id"),
    "feature_id" BIGINT,
    CONSTRAINT fk_feature_of_interest FOREIGN KEY("feature_id") REFERENCES public.feature_of_interest("id")
);

SELECT create_hypertable('public.observation_array', 'phenomenon_time', create_default_indexes=>FALSE);

CREATE UNIQUE INDEX idx_observation_array
  ON public.observation_array(id, phenomenon_time);

CREATE INDEX ON public.observation_array (quality_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_array (datastream_id, phenomenon_time DESC);
CREATE INDEX ON public.observation_array (feature_id, phenomenon_time DESC);