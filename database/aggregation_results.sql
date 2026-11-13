-- Stored aggregation results for the future Python worker. These are ordinary
-- rowstore hypertables, not continuous aggregates. No policies/jobs are created.
-- Apply after the istSOS schema, as the database owner/superuser. Re-applicable.

CREATE SCHEMA IF NOT EXISTS aggregation AUTHORIZATION administrator;

SET ROLE administrator;

DO $results$
DECLARE
    definition RECORD;
BEGIN
    -- Storage chunk widths are operational defaults, independent of bucket
    -- durations. They can be tuned later with set_chunk_time_interval().
    FOR definition IN
        SELECT * FROM (VALUES
            ('observations_10min', INTERVAL '7 days'),
            ('observations_1hour', INTERVAL '30 days'),
            ('observations_1day', INTERVAL '365 days'),
            ('observations_7days', INTERVAL '365 days'),
            ('observations_1month', INTERVAL '3650 days'),
            ('observations_1year', INTERVAL '3650 days')
        ) AS intervals(table_name, chunk_width)
    LOOP
        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS aggregation.%I (
                datastream_id BIGINT NOT NULL
                    REFERENCES sensorthings."Datastream" (id) ON DELETE CASCADE,
                bucket_start TIMESTAMPTZ NOT NULL CHECK (isfinite(bucket_start)),
                bucket_end TIMESTAMPTZ NOT NULL CHECK (isfinite(bucket_end)),
                evaluated_until TIMESTAMPTZ NOT NULL CHECK (isfinite(evaluated_until)),
                count_total BIGINT NOT NULL CHECK (count_total >= 0),
                count_accepted BIGINT NOT NULL,
                count_numeric BIGINT NOT NULL,
                sum_value DOUBLE PRECISION,
                min_value DOUBLE PRECISION,
                max_value DOUBLE PRECISION,
                avg_value DOUBLE PRECISION,
                expected_count BIGINT CHECK (expected_count >= 0),
                coverage DOUBLE PRECISION,
                result_quality BIGINT NOT NULL CHECK (result_quality >= 0),
                constraint_count SMALLINT NOT NULL CHECK (constraint_count BETWEEN 0 AND 28),
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    CHECK (isfinite(calculated_at)),
                configuration_snapshot JSONB NOT NULL
                    CHECK (jsonb_typeof(configuration_snapshot) = 'object'),
                PRIMARY KEY (datastream_id, bucket_start),
                CHECK (bucket_end > bucket_start),
                CHECK (evaluated_until BETWEEN bucket_start AND bucket_end),
                CHECK (count_accepted BETWEEN 0 AND count_total),
                CHECK (count_numeric BETWEEN 0 AND count_accepted),
                CHECK (
                    (count_numeric = 0 AND sum_value IS NULL AND min_value IS NULL
                        AND max_value IS NULL AND avg_value IS NULL)
                    OR
                    (count_numeric > 0 AND sum_value IS NOT NULL AND min_value IS NOT NULL
                        AND max_value IS NOT NULL AND avg_value IS NOT NULL)
                ),
                CHECK (sum_value > '-Infinity'::double precision AND sum_value < 'Infinity'::double precision),
                CHECK (min_value > '-Infinity'::double precision AND min_value < 'Infinity'::double precision),
                CHECK (max_value > '-Infinity'::double precision AND max_value < 'Infinity'::double precision),
                CHECK (avg_value > '-Infinity'::double precision AND avg_value < 'Infinity'::double precision),
                CHECK (min_value <= max_value),
                CHECK (CASE WHEN expected_count IS NULL OR expected_count = 0
                    THEN coverage IS NULL
                    ELSE coverage IS NOT NULL AND coverage >= 0
                        AND coverage < 'Infinity'::double precision
                    END),
                -- Data + N automatic checks + human + aggregation status.
                -- N <= 28 keeps the packed mask within a nonnegative BIGINT.
                CHECK (result_quality >> (2 * (constraint_count + 3)) = 0)
            )
        $ddl$, definition.table_name);

        PERFORM create_hypertable(
            format('aggregation.%I', definition.table_name)::regclass,
            by_range('bucket_start', definition.chunk_width),
            if_not_exists => true
        );

        EXECUTE format(
            'COMMENT ON TABLE aggregation.%I IS %L',
            definition.table_name,
            'Latest computed aggregate state, shared by source Datastreams. '
            'Statistics are calculated by the worker; no automatic aggregation or version history. '
            'No FK to configuration, so removing settings preserves results.'
        );
        EXECUTE format(
            'COMMENT ON COLUMN aggregation.%I.configuration_snapshot IS %L',
            definition.table_name,
            'Effective calculation settings and source QC metadata used for this result.'
        );
        EXECUTE format(
            'GRANT ALL ON aggregation.%I TO administrator', definition.table_name
        );
    END LOOP;
END;
$results$;

RESET ROLE;

GRANT USAGE ON SCHEMA aggregation TO administrator;
