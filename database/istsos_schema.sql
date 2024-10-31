CREATE EXTENSION IF NOT exists postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA sensorthings;

CREATE TABLE IF NOT EXISTS sensorthings."Location" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "location" geometry NOT NULL,
    "properties" jsonb DEFAULT NULL,
    "gen_foi_id" BIGINT
);

SELECT UpdateGeometrySRID('sensorthings', 'Location', 'location', current_setting('custom.epsg')::int);

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')/Things';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "HistoricalLocations@iot.navigationLink"(sensorthings."Location") RETURNS text AS $$
    SELECT '/Locations(' || $1.id || ')/HistoricalLocations';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Thing" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "properties" jsonb DEFAULT NULL
);

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
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_historicallocation_thing_id" ON sensorthings."HistoricalLocation" USING btree ("thing_id" ASC NULLS LAST) TABLESPACE pg_default;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Locations@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')/Locations';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Thing@iot.navigationLink"(sensorthings."HistoricalLocation") RETURNS text AS $$
    SELECT '/HistoricalLocations(' || $1.id || ')/Thing';
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
    "properties" jsonb DEFAULT NULL
);

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."ObservedProperty") RETURNS text AS $$
    SELECT '/ObservedProperties(' ||  $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."ObservedProperty") RETURNS text AS $$
    SELECT '/ObservedProperties(' || $1.id || ')/Datastreams';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Sensor" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "metadata" VARCHAR(255) NOT NULL,
    "properties" jsonb DEFAULT NULL
);

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Sensor") RETURNS text AS $$
    SELECT '/Sensors(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Sensor") RETURNS text AS $$
    SELECT '/Sensors(' || $1.id || ')/Datastreams';
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS sensorthings."Datastream" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "unitOfMeasurement" jsonb NOT NULL,
    "observationType" VARCHAR(100) NOT NULL,
    "observedArea" geometry,
    "phenomenonTime" tstzrange,
    "resultTime" tstzrange,
    "properties" jsonb DEFAULT NULL,
    "thing_id" BIGINT NOT NULL REFERENCES sensorthings."Thing"(id) ON DELETE CASCADE,
    "sensor_id" BIGINT NOT NULL REFERENCES sensorthings."Sensor"(id) ON DELETE CASCADE,
    "observedproperty_id" BIGINT NOT NULL REFERENCES sensorthings."ObservedProperty"(id) ON DELETE CASCADE,
    "last_foi_id" BIGINT
);

SELECT UpdateGeometrySRID('sensorthings', 'Datastream', 'observedArea', current_setting('custom.epsg')::int);


CREATE INDEX IF NOT EXISTS "idx_datastream_thing_id" ON sensorthings."Datastream" USING btree ("thing_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_datastream_sensor_id" ON sensorthings."Datastream" USING btree ("sensor_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_datastream_observedproperty_id" ON sensorthings."Datastream" USING btree ("observedproperty_id" ASC NULLS LAST) TABLESPACE pg_default;

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

CREATE TABLE IF NOT EXISTS sensorthings."FeaturesOfInterest" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT NOT NULL,
    "encodingType" VARCHAR(100) NOT NULL,
    "feature" geometry NOT NULL,
    "properties" jsonb DEFAULT NULL
);

SELECT UpdateGeometrySRID('sensorthings', 'FeaturesOfInterest', 'feature', current_setting('custom.epsg')::int);

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."FeaturesOfInterest") RETURNS text AS $$
    SELECT '/FeaturesOfInterest(' || $1.id || ')/Observations';
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
    "featuresofinterest_id" BIGINT NOT NULL REFERENCES sensorthings."FeaturesOfInterest"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_observation_datastream_id" ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_featuresofinterest_id" ON sensorthings."Observation" USING btree ("featuresofinterest_id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_observation_id_datastream_id" ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST, "id" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "idx_observation_datastream_id_phtime" ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST, "phenomenonTime" ASC NULLS LAST, "id" ASC NULLS LAST) TABLESPACE pg_default WHERE "datastream_id" IS NOT NULL;

CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "FeatureOfInterest@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')/FeatureOfInterest';
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION "Datastream@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
    SELECT '/Observations(' || $1.id || ')/Datastream';
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

DO $$
BEGIN
    IF NOT current_setting('custom.duplicates', false)::boolean THEN
        -- Add the UNIQUE constraint on the 'name' column
        EXECUTE 'ALTER TABLE sensorthings."Location"
                 ADD CONSTRAINT unique_location_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."Thing"
                 ADD CONSTRAINT unique_thing_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."ObservedProperty"
                 ADD CONSTRAINT unique_observedProperty_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."Sensor"
                 ADD CONSTRAINT unique_sensor_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."Datastream"
                 ADD CONSTRAINT unique_datastream_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."FeaturesOfInterest"
                 ADD CONSTRAINT unique_featuresOfInterest_name UNIQUE ("name");';

        EXECUTE 'ALTER TABLE sensorthings."Observation"
                 ADD CONSTRAINT unique_observation_phenomenonTime_datastreamId UNIQUE ("phenomenonTime", "datastream_id");';
    END IF;
END $$;

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

CREATE OR REPLACE FUNCTION sensorthings.expand(
    query_ text,
    fk_field_ text,
    fk_id_ integer,
    limit_ integer DEFAULT 101,
    offset_ integer DEFAULT 0,
    one_to_many_ boolean DEFAULT true,
	show_id boolean DEFAULT false,
    table_ text DEFAULT ''::text,
    base_url_ text DEFAULT ''::text,
    is_count boolean DEFAULT false,
    count_mode_ text DEFAULT 'FULL'::text,
	count_estimate_threshold_ integer DEFAULT 10000)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result jsonb;
    next_link text;
    where_clause text;
	id_column text := CASE WHEN show_id THEN 'id' ELSE 'id as "@iot.id"' END;
    preliminary_count integer;
	total_count integer;
BEGIN

    IF one_to_many_ THEN
        EXECUTE format(
			'SELECT COALESCE
				(
					(
						SELECT jsonb_agg(row_to_json(%I)::jsonb - %L - %L)
						FROM (
							SELECT %s, *
							FROM (%s) d
							WHERE d.%s = %s
							LIMIT %s OFFSET %s
						) %I
					), ''[]''
				)',
			table_, fk_field_, 'id', id_column, query_, fk_field_, fk_id_, limit_, offset_, table_
		) INTO result;

        IF jsonb_array_length(result) > limit_ - 1 THEN
            result := result - (limit_ - 1);
            next_link := base_url_ || table_ || '?$top=' || limit_ - 1 || '&$skip=' || (offset_ + limit_ - 1);
        ELSE
            next_link := NULL;
        END IF;

        IF is_count THEN
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
            RETURN json_build_object(table_, result, table_ || '@iot.nextLink', next_link, table_ || '@iot.count', total_count);
        END IF;
        
        RETURN json_build_object(table_, result, table_ || '@iot.nextLink', next_link);

    ELSE
		-- Adjust where_clause based on whether fk_id_ is NULL
		IF fk_id_ IS NULL THEN
			where_clause := format('d.%s IS NULL', fk_field_);
		ELSE
			where_clause := format('d.%s = %s', fk_field_, fk_id_);
		END IF;

        EXECUTE format(
			'SELECT COALESCE
				(
					(
						SELECT row_to_json(%I)::jsonb - %L
						FROM (
							SELECT %s, * 
							FROM (%s) d 
							WHERE %s
							LIMIT %s OFFSET %s
						) %I
					), ''{}''::jsonb
				)',
			table_, 'id', id_column, query_, where_clause, limit_ - 1, offset_, table_
		) INTO result;
		
        RETURN result;
	END IF;

END;
$BODY$;

CREATE OR REPLACE FUNCTION sensorthings.expand_many2many(
	query_ text,
	join_table_ text,
	fk_id_ integer,
	related_fk_field1_ text,
	related_fk_field2_ text,
	limit_ integer DEFAULT 101,
	offset_ integer DEFAULT 0,
    show_id boolean DEFAULT false,
    table_ text DEFAULT ''::text,
    base_url_ text DEFAULT ''::text,
    is_count boolean DEFAULT false,
    count_mode_ text DEFAULT 'FULL'::text,
	count_estimate_threshold_ integer DEFAULT 10000)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result jsonb;
    next_link text;
    id_column text := CASE WHEN show_id THEN 'm.id' ELSE 'm.id as "@iot.id"' END;
    preliminary_count integer;
	total_count integer;
BEGIN
    EXECUTE format(
        'SELECT COALESCE
			(
				(
					SELECT jsonb_agg(row_to_json(%I)::jsonb - %L)
			        FROM (
						SELECT %s, m.* 
				      	FROM (%s) m
				      	JOIN %s jt ON m.id = jt.%s
				      	WHERE jt.%s = %s
				      	LIMIT %s OFFSET %s
					) %I
				), ''[]''
			)',
			table_, 'id', id_column, query_, join_table_, related_fk_field1_, related_fk_field2_, fk_id_, limit_, offset_, table_
    ) INTO result;

    IF jsonb_array_length(result) > limit_ - 1 THEN
        result := result - (limit_ - 1);
        next_link := base_url_ || table_ || '?$top=' || limit_ - 1 || '&$skip=' || (offset_ + limit_ - 1);
    ELSE
        next_link := NULL;
    END IF;

    IF is_count THEN
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
        RETURN json_build_object(table_, result, table_ || '@iot.nextLink', next_link, table_ || '@iot.count', total_count);
    END IF;
    
    RETURN json_build_object(table_, result, table_ || '@iot.nextLink', next_link);
END;
$BODY$;