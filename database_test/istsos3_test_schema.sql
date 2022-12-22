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
    "properties" jsonb,
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
CREATE TABLE IF NOT EXISTS public.unit_measure (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "unit_name" character varying(255) NOT NULL,
    "unit_symbol" character varying(255),
    "unit_definition" character varying(255)
);
-- DATASTREAM
CREATE TABLE IF NOT EXISTS public.datastream (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" text UNIQUE NOT NULL,
    "description" text,
    "observed_area" geometry(geometry, 4326) NOT NULL,
    "phenomenon_time_start" timestamp with time zone,
    "phenomenon_time_end" timestamp with time zone,
    "result_time_start" timestamp with time zone,
    "result_time_end" timestamp with time zone,
    "properties" jsonb,
    "constraints" jsonb,
    "sensor_id" uuid REFERENCES public.sensor ("id"),
    "observed_property_id" uuid REFERENCES public.observed_property ("id"),
    "thing_id" BIGINT,
    CONSTRAINT fk_thing FOREIGN KEY("thing_id") REFERENCES public.thing("id"),
    "unit_measure_id" BIGINT,
    CONSTRAINT fk_unit_measure FOREIGN KEY("unit_measure_id") REFERENCES public.unit_measure("id")
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
-- OBSERVATION
CREATE TABLE IF NOT EXISTS public.observation (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "phenomenon_time_start" timestamp with time zone,
    "phenomenon_time_end" timestamp with time zone,
    "result_time_start" timestamp with time zone,
    "result_time_end" timestamp with time zone,
    "valid_time_start" timestamp with time zone,
    "valid_time_end" timestamp with time zone,
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
-- OBSERVATION INHERITANCE
CREATE TABLE IF NOT EXISTS public.observation_json ("result_json" jsonb) INHERITS (public.observation);
CREATE TABLE IF NOT EXISTS public.observation_boolean ("result_boolean" boolean) INHERITS (public.observation);
CREATE TABLE IF NOT EXISTS public.observation_number ("result_number" double precision) INHERITS (public.observation);
CREATE TABLE IF NOT EXISTS public.observation_string ("result_string" text) INHERITS (public.observation);
CREATE TABLE IF NOT EXISTS public.observation_array ("result_array" float []) INHERITS (public.observation);