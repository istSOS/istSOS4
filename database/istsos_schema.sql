CREATE EXTENSION IF NOT exists postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA sensorthings;

CREATE TABLE IF NOT EXISTS sensorthings."Commit"(
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "author" VARCHAR(255) NOT NULL,
    "encodingType" VARCHAR(100),
    "message" VARCHAR(255) NOT NULL,
    "date" TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/Thing';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Location@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/Location';

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "HistoricalLocation@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/HistoricalLocation';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "ObservedProperty@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/ObservedProperty';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Sensor@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/Sensor';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/Datastream';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/FeatureOfInterest';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Observation@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
    SELECT '/Commits(' || $1.id || ')/Observation';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Location" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "location" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE,
    "gen_foi_id" BIGINT
);

CREATE INDEX IF NOT EXISTS "idx_location_commit_id" ON sensorthings."Location" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')/Things';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')/HistoricalLocations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Thing" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_thing_commit_id" ON sensorthings."Thing" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Thing") RETURNS text AS $$
    SELECT '/Things(' || $1.id || ')';

$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
    SELECT '/Things(' || $1.id || ')/Locations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
    SELECT '/Things(' || $1.id || ')/HistoricalLocations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
    SELECT '/Things(' || $1.id || ')/Datastreams';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
    SELECT '/Things(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Thing_Location" (
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "location_id" BIGINT NOT NULL REFERENCES sensorthings."Location"(id) ON DELETE CASCADE,
    CONSTRAINT thing_location_unique UNIQUE ("thing_id", "location_id")
);

CREATE INDEX IF NOT EXISTS "idx_thing_location_thing_id" ON sensorthings."Thing_Location" USING btree ("thing_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_thing_location_location_id" ON sensorthings."Thing_Location" USING btree ("location_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS sensorthings."HistoricalLocation" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "time" TIMESTAMPTZ DEFAULT NOW(),
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_historicallocation_thing_id" ON sensorthings."HistoricalLocation" USING btree ("thing_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_historicallocation_commit_id" ON sensorthings."HistoricalLocation" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')/Locations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')/Thing';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Location_HistoricalLocation" (
    "location_id" BIGINT NOT NULL REFERENCES sensorthings."Location"(id) ON DELETE CASCADE,
    "historicallocation_id" BIGINT NOT NULL REFERENCES sensorthings."HistoricalLocation"(id) ON DELETE CASCADE,
    CONSTRAINT location_historical_location_unique UNIQUE ("location_id", "historicallocation_id")
);

CREATE INDEX IF NOT EXISTS "idx_location_historicallocation_location_id" ON sensorthings."Location_HistoricalLocation" USING btree ("location_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_location_historicallocation_historicallocation_id" ON sensorthings."Location_HistoricalLocation" USING btree ("historicallocation_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS sensorthings."ObservedProperty" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "definition" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_observedproperty_commit_id" ON sensorthings."ObservedProperty" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."ObservedProperty") RETURNS text AS $$
    SELECT '/ObservedProperties(' ||  $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS text AS $$
    SELECT '/ObservedProperties(' || $1.id || ')/Datastreams';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS text AS $$
    SELECT '/ObservedProperties(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" VARCHAR(255) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_sensor_commit_id" ON sensorthings."Sensor" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Sensor") RETURNS text AS $$
    SELECT '/Sensors(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Sensor") RETURNS text AS $$
    SELECT '/Sensors(' || $1.id || ')/Datastreams';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Sensor") RETURNS text AS $$
    SELECT '/Sensors(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

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
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_datastream_thing_id" ON sensorthings."Datastream" USING btree ("thing_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_datastream_sensor_id" ON sensorthings."Datastream" USING btree ("sensor_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_datastream_observedproperty_id" ON sensorthings."Datastream" USING btree ("observedproperty_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_datastream_commit_id" ON sensorthings."Datastream" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')/Thing';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Sensor@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')/Sensor';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "ObservedProperty@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')/ObservedProperty';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')/Observations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
    SELECT '/Datastreams(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."FeaturesOfInterest" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "feature" geometry(geometry, 4326) NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_featuresofinterest_commit_id" ON sensorthings."FeaturesOfInterest" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')/Observations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

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
    "commit_id" BIGINT REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_observation_datastream_id" ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_featuresofinterest_id" ON sensorthings."Observation" USING btree ("featuresofinterest_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_observation_id_datastream_id" ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST, "id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_commit_id" ON sensorthings."Observation" USING btree ("commit_id" ASC NULLS LAST) TABLESPACE pg_default;


CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')/FeatureOfInterest';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')/Datastream';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')/Commit';
$$ LANGUAGE SQL;

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

CREATE OR REPLACE FUNCTION sensorthings.count_estimate(
	query text)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
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
$BODY$;

CREATE OR REPLACE FUNCTION sensorthings.basic_expand(
    query_ text,
    fk_field_ text,
    fk_id_ integer,
    limit_ integer DEFAULT 100,
    offset_ integer DEFAULT 0,
    one_to_many_ boolean DEFAULT true,
	show_id boolean DEFAULT false)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result json;
BEGIN
    IF one_to_many_ THEN

        IF show_id THEN
	        -- Execute the query for one-to-many relationship and aggregate the result
	        EXECUTE format(
	            'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L - %L), ''[]'')
	            FROM (SELECT * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            fk_field_, 'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		ELSE
			-- Execute the query for one-to-many relationship and aggregate the result
	        EXECUTE format(
	            'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L - %L), ''[]'')
	            FROM (SELECT id as "@iot.id", * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            fk_field_, 'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		END IF;

    ELSE

        IF show_id THEN
	        -- Execute the query for many-to-one relationship
	        EXECUTE format(
	            'SELECT COALESCE(row_to_json(t)::jsonb - %L, ''{}'')
	            FROM (SELECT * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		ELSE
			-- Execute the query for many-to-one relationship
	        EXECUTE format(
	            'SELECT COALESCE(row_to_json(t)::jsonb - %L, ''{}'')
	            FROM (SELECT id as "@iot.id", * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		END IF;
		
	END IF;

    -- Return the result
    RETURN result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION sensorthings.advanced_expand(
	query_ text,
	fk_field_ text,
	fk_id_ integer,
	limit_ integer DEFAULT 101,
	offset_ integer DEFAULT 0,
	one_to_many_ boolean DEFAULT true,
	show_id boolean DEFAULT false,
	table_ text DEFAULT ''::text,
	count_mode_ text DEFAULT 'FULL'::text,
	count_estimate_threshold_ integer DEFAULT 10000,
	is_count_ boolean DEFAULT false)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result json;
    preliminary_count integer;
    total_count integer;
	base_json jsonb;
	result_length integer;
BEGIN
    IF one_to_many_ THEN

		IF show_id THEN
	        -- Execute the query for one-to-many relationship and aggregate the result
	        EXECUTE format(
	            'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L - %L), ''[]'')
	            FROM (SELECT * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            fk_field_, 'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		ELSE
			-- Execute the query for one-to-many relationship and aggregate the result
	        EXECUTE format(
	            'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L - %L), ''[]'')
	            FROM (SELECT id as "@iot.id", * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            fk_field_, 'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		END IF;

		base_json := jsonb_build_object('data', result);

		-- Get the length of the result (number of items in the array)
        result_length := jsonb_array_length(result::jsonb);

		-- Return result with or without pagination
		IF result_length > limit_ - 1 THEN
			result := result::jsonb - (jsonb_array_length(result::jsonb) - 1);
			base_json := jsonb_build_object('data', result);
			base_json := base_json || jsonb_build_object(
				table_ || '@iot.nextLink', table_ || '?$top=' || limit_ - 1 || '&$skip=' || (offset_ + limit_ - 1)
			);
		END IF;

		-- Add count information if is_count_ is true
	    IF is_count_ THEN

	        -- Handle count based on count_mode_
	        IF count_mode_ = 'FULL' THEN
	            -- Full count mode: get the exact count
	            EXECUTE format(
	                'SELECT COUNT(*) FROM (%s) d WHERE d.%s = %s',
	                query_, fk_field_, fk_id_
	            ) INTO total_count;
	        
	        ELSIF count_mode_ = 'LIMIT_ESTIMATE' THEN
	            -- Limit estimate mode: get preliminary count with a limit
	            EXECUTE format(
	                'SELECT COUNT(*) FROM (%s) d WHERE d.%s = %s LIMIT %s',
	                query_, fk_field_, fk_id_, count_estimate_threshold_
	            ) INTO preliminary_count;

	            IF preliminary_count == count_estimate_threshold_ THEN
	                -- Use count estimate if preliminary count exceeds threshold
	                EXECUTE format(
	                    'SELECT sensorthings.count_estimate(
	                        ''SELECT 1 FROM (%s) d WHERE d.%s = %s'')',
	                    replace(query_, '''', ''''''), fk_field_, fk_id_
	                ) INTO total_count;
	            ELSE
	                -- Use preliminary count if below threshold
	                total_count := preliminary_count;
	            END IF;

	        ELSIF count_mode_ = 'ESTIMATE_LIMIT' THEN
	            -- Estimate limit mode: perform count estimate first
	            EXECUTE format(
	                'SELECT sensorthings.count_estimate(
	                    ''SELECT 1 FROM (%s) d WHERE d.%s = %s'')',
	                replace(query_, '''', ''''''), fk_field_, fk_id_
	            ) INTO total_count;

	            -- Perform a precise count with limit if the estimate is below threshold
	            IF total_count < count_estimate_threshold_ THEN
	                EXECUTE format(
	                    'SELECT COUNT(*) FROM (%s) d WHERE d.%s = %s LIMIT %s',
	                    query_, fk_field_, fk_id_, count_estimate_threshold_
	                ) INTO total_count;
	            END IF;
	        END IF;

	        base_json := base_json || jsonb_build_object(
	            table_ || '@iot.count', total_count
	        );
	    END IF;

		RETURN base_json;
    ELSE

		IF show_id THEN
	        -- Execute the query for many-to-one relationship
	        EXECUTE format(
	            'SELECT COALESCE(row_to_json(t)::jsonb - %L, ''{}'')
	            FROM (SELECT * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		ELSE
			-- Execute the query for many-to-one relationship
	        EXECUTE format(
	            'SELECT COALESCE(row_to_json(t)::jsonb - %L, ''{}'')
	            FROM (SELECT id as "@iot.id", * 
	                  FROM (%s) d 
	                  WHERE d.%s = %s 
	                  LIMIT %s OFFSET %s) t',
	            'id', query_, fk_field_, fk_id_, limit_, offset_
	        ) INTO result;
		END IF;

        RETURN result;
    END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION sensorthings.basic_expand_many2many(
	query_ text,
	join_table_ text,
	fk_id_ integer,
	related_fk_field1_ text,
	related_fk_field2_ text,
	limit_ integer DEFAULT 100,
	offset_ integer DEFAULT 0,
    show_id boolean DEFAULT false)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    query text;
    result json;
BEGIN
	IF show_id THEN
	    -- Execute the query for many-to-many relationship and aggregate the result
	    EXECUTE format(
	        'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L), ''[]'')
	        FROM (SELECT m.* 
	              FROM (%s) m
	              JOIN %s jt ON m.id = jt.%s
	              WHERE jt.%s = %s
	              LIMIT %s OFFSET %s) t',
	        'id', query_, join_table_, related_fk_field1_, related_fk_field2_, fk_id_, limit_, offset_
	    ) INTO result;
	ELSE
		EXECUTE format(
	        'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L), ''[]'')
	        FROM (SELECT m.id as "@iot.id", m.* 
	              FROM (%s) m
	              JOIN %s jt ON m.id = jt.%s
	              WHERE jt.%s = %s
	              LIMIT %s OFFSET %s) t',
	        'id', query_, join_table_, related_fk_field1_, related_fk_field2_, fk_id_, limit_, offset_
	    ) INTO result;
	END IF;

    RETURN result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION sensorthings.advanced_expand_many2many(
	query_ text,
	join_table_ text,
	fk_id_ integer,
	related_fk_field1_ text,
	related_fk_field2_ text,
	limit_ integer DEFAULT 101,
	offset_ integer DEFAULT 0,
	show_id boolean DEFAULT false,
	table_ text DEFAULT ''::text,
	count_mode_ text DEFAULT 'FULL'::text,
	count_estimate_threshold_ integer DEFAULT 10000,
	is_count_ boolean DEFAULT false)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result json;
    preliminary_count integer;
    total_count integer;
	base_json jsonb;
	result_length integer;
BEGIN

	IF show_id THEN
	    -- Execute the query for many-to-many relationship and aggregate the result
	    EXECUTE format(
	        'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L), ''[]'')
	        FROM (SELECT m.* 
	              FROM (%s) m
	              JOIN %s jt ON m.id = jt.%s
	              WHERE jt.%s = %s
	              LIMIT %s OFFSET %s) t',
	        'id', query_, join_table_, related_fk_field1_, related_fk_field2_, fk_id_, limit_, offset_
	    ) INTO result;
	ELSE
		EXECUTE format(
	        'SELECT COALESCE(jsonb_agg(row_to_json(t)::jsonb - %L), ''[]'')
	        FROM (SELECT m.id as "@iot.id", m.* 
	              FROM (%s) m
	              JOIN %s jt ON m.id = jt.%s
	              WHERE jt.%s = %s
	              LIMIT %s OFFSET %s) t',
	        'id', query_, join_table_, related_fk_field1_, related_fk_field2_, fk_id_, limit_, offset_
	    ) INTO result;
	END IF;

	base_json := jsonb_build_object('data', result);

	-- Get the length of the result (number of items in the array)
	result_length := jsonb_array_length(result::jsonb);

	-- Return result with or without pagination
	IF result_length > limit_ - 1 THEN
		result := result::jsonb - (jsonb_array_length(result::jsonb) - 1);
		base_json := jsonb_build_object('data', result);
		base_json := base_json || jsonb_build_object(
			table_ || '@iot.nextLink', table_ || '?$top=' || limit_ - 1 || '&$skip=' || (offset_ + limit_ - 1)
		);
	END IF;

	-- Add count information if is_count_ is true
	IF is_count_ THEN

		-- Handle count based on count_mode_
		IF count_mode_ = 'FULL' THEN
			-- Full count mode: get the exact count
			EXECUTE format(
				'SELECT COUNT(*) FROM (%s) m 
				JOIN %s jt ON m.id = jt.%s
				WHERE jt.%s = %s',
				query_,
				join_table_,
				related_fk_field1_,
				related_fk_field2_,
				fk_id_
			) INTO total_count;

		ELSIF count_mode_ = 'LIMIT_ESTIMATE' THEN
			-- Limit estimate mode: get preliminary count with a limit
			EXECUTE format(
				'SELECT COUNT(*) FROM (%s) m 
				JOIN %s jt ON m.id = jt.%s
				WHERE jt.%s = %s
				LIMIT %s',
				query_,
				join_table_,
				related_fk_field1_,
				related_fk_field2_,
				fk_id_,
				count_estimate_threshold_
			) INTO preliminary_count;

			IF preliminary_count = count_estimate_threshold_ THEN
				-- Use count estimate if preliminary count exceeds threshold
				EXECUTE format(
					'SELECT sensorthings.count_estimate(''SELECT 1 FROM (%s) m 
					JOIN %s jt ON m.id = jt.%s
					WHERE jt.%s = %s'')',
					replace(query_, '''', ''''''),
					join_table_,
					related_fk_field1_,
					related_fk_field2_,
					fk_id_
				) INTO total_count;
			ELSE
				-- Use preliminary count if below threshold
				total_count := preliminary_count;
			END IF;

		ELSIF count_mode_ = 'ESTIMATE_LIMIT' THEN
			-- Estimate limit mode: perform count estimate first
			EXECUTE format(
				'SELECT sensorthings.count_estimate(''SELECT 1 FROM (%s) m 
				JOIN %s jt ON m.id = jt.%s
				WHERE jt.%s = %s'')',
				replace(query_, '''', ''''''),
				join_table_,
				related_fk_field1_,
				related_fk_field2_,
				fk_id_
			) INTO total_count;

			-- Perform a precise count with limit if the estimate is below threshold
			IF total_count < count_estimate_threshold_ THEN
				EXECUTE format(
					'SELECT COUNT(*) FROM (%s) m 
					JOIN %s jt ON m.id = jt.%s
					WHERE jt.%s = %s
					LIMIT %s',
					query_,
					join_table_,
					related_fk_field1_,
					related_fk_field2_,
					fk_id_,
					count_estimate_threshold_
				) INTO total_count;
			END IF;
		END IF;

		base_json := base_json || jsonb_build_object(
			table_ || '@iot.count', total_count
		);
	END IF;

	RETURN base_json;
END;
$BODY$;
