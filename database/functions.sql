--------------------------------------------------------------------------------
-- istSOS4 / SensorThings aggregation utilities
--
-- Purpose:
--   - Create aggregated Observations in a target Datastream.
--   - Support standard aggregation intervals: 10 min, 1 hour, 1 day, 1 year.
--   - Support explicit historical/backfill windows using window_start/window_end.
--   - Support SUM, AVG, MIN, MAX, COUNT and SUM_NULL_AS_ONE.
--   - Support optional filtering by resultQuality.
--   - Store aggregated resultQuality as MIN(resultQuality) of the observations
--     actually used by the aggregation.
--   - If a bucket contains no valid observations:
--         resultNumber  = -999.9
--         resultQuality = 0
--   - Optionally stop advancing when the source station is no longer active.
--   - Optionally emit zero for an empty event/tick bucket while the station is
--     known to be active through one or more availability Datastreams.
--   - Optionally recompute historical buckets affected by source or
--     availability Observation updates detected through systemTimeValidity.
--   - Incrementally update Datastream.phenomenonTime, resultTime and last_foi_id.
--
-- Expected Observation uniqueness:
--
--   UNIQUE (
--       "phenomenonTimeStart",
--       "phenomenonTimeEnd",
--       "datastream_id"
--   )
--
-- Boundary modes:
--
--   right_closed = (start, end]
--   left_closed  = [start, end)
--
-- Normal operation:
--
--   The TimescaleDB job calls one of the wrapper procedures and normally
--   recomputes only the last N closed buckets.
--
-- Historical maintenance:
--
--   window_start + window_end can be passed in the config to recompute a
--   complete historical window. Every bucket in the window is rewritten.
--
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- 1. Helper function: extract a numeric resultQuality value from JSONB.
--
-- Supported forms:
--
--   0
--   "0"
--   {"code": 0}
--   {"code": "0"}
--
-- If p_quality_key is NULL, resultQuality is expected to be a scalar.
-- If p_quality_key is specified, that key is extracted from the JSON object.
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sensorthings._quality_value_as_double(
    p_result_quality jsonb,
    p_quality_key text DEFAULT NULL
)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_text text;
BEGIN
    IF p_result_quality IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_quality_key IS NULL OR btrim(p_quality_key) = '' THEN
        -- Scalar JSONB value.
        v_text := p_result_quality #>> '{}';
    ELSE
        -- JSON object value.
        v_text := p_result_quality ->> p_quality_key;
    END IF;

    IF v_text IS NULL OR btrim(v_text) = '' THEN
        RETURN NULL;
    END IF;

    RETURN v_text::double precision;

EXCEPTION
    WHEN invalid_text_representation
      OR numeric_value_out_of_range THEN
        RETURN NULL;
END;
$$;


--------------------------------------------------------------------------------
-- Supporting index for incremental system-time lookups.
--
-- systemTimeValidity is installed by the optional istSOS versioning schema, so
-- create the index only when the column is present. On a TimescaleDB hypertable
-- this creates the corresponding index on its chunks as well.
--------------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'sensorthings'
          AND table_name = 'Observation'
          AND column_name = 'systemTimeValidity'
    ) THEN
        EXECUTE $index$
            CREATE INDEX IF NOT EXISTS
                "idx_observation_datastream_system_time"
            ON sensorthings."Observation" (
                "datastream_id",
                lower("systemTimeValidity")
            )
        $index$;
    END IF;
END;
$$;


--------------------------------------------------------------------------------
-- Internal helper: resolve buckets affected by system-time changes.
--
-- The query is dynamic so installations without the optional
-- Observation.systemTimeValidity column can still use every non-incremental
-- aggregation feature. aggregate_datastream validates the column before it
-- invokes this helper with a non-NULL system-time boundary.
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sensorthings._changed_observation_buckets(
    p_source_datastream_id bigint,
    p_bucket_interval interval,
    p_bucket_timezone text,
    p_boundary_mode text,
    p_system_time_from timestamptz,
    p_system_time_to timestamptz
)
RETURNS TABLE(bucket_start timestamptz)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_system_time_from IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY EXECUTE $query$
        SELECT DISTINCT
            time_bucket(
                $2,
                o."phenomenonTimeStart"
                    - CASE
                        WHEN $4 = 'right_closed'
                            THEN interval '1 microsecond'
                        ELSE interval '0 seconds'
                      END,
                timezone => $3
            ) AS bucket_start

        FROM sensorthings."Observation" o

        WHERE o."datastream_id" = $1
          AND lower(o."systemTimeValidity") >= $5
          AND lower(o."systemTimeValidity") < $6
          AND o."phenomenonTimeStart" IS NOT NULL
          AND time_bucket(
                  $2,
                  o."phenomenonTimeStart"
                      - CASE
                          WHEN $4 = 'right_closed'
                              THEN interval '1 microsecond'
                          ELSE interval '0 seconds'
                        END,
                  timezone => $3
              ) + $2
              <= time_bucket($2, $6, timezone => $3)
    $query$
    USING
        p_source_datastream_id,
        p_bucket_interval,
        p_bucket_timezone,
        p_boundary_mode,
        p_system_time_from,
        p_system_time_to;
END;
$$;


--------------------------------------------------------------------------------
-- Internal helper: resolve event/tick buckets affected by newly inserted or
-- updated availability Observations.
--
-- One availability Observation can prove activity for multiple bucket ends,
-- from its phenomenon time through availability_max_age. The generated bucket
-- starts are filtered to exactly that interval and to closed buckets.
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sensorthings._changed_availability_buckets(
    p_availability_datastream_ids bigint[],
    p_bucket_interval interval,
    p_bucket_timezone text,
    p_system_time_from timestamptz,
    p_system_time_to timestamptz,
    p_availability_max_age interval
)
RETURNS TABLE(bucket_start timestamptz)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_system_time_from IS NULL
       OR cardinality(p_availability_datastream_ids) = 0 THEN
        RETURN;
    END IF;

    RETURN QUERY EXECUTE $query$
        SELECT DISTINCT
            b.bucket_start

        FROM sensorthings."Observation" o

        CROSS JOIN LATERAL generate_series(
            time_bucket($2, o."phenomenonTimeStart", timezone => $3) - $2,
            time_bucket(
                $2,
                o."phenomenonTimeStart" + $6,
                timezone => $3
            ),
            $2
        ) b(bucket_start)

        WHERE o."datastream_id" = ANY($1)
          AND lower(o."systemTimeValidity") >= $4
          AND lower(o."systemTimeValidity") < $5
          AND o."phenomenonTimeStart" IS NOT NULL
          AND b.bucket_start + $2 >= o."phenomenonTimeStart"
          AND b.bucket_start + $2
              <= o."phenomenonTimeStart" + $6
          AND b.bucket_start + $2
              <= time_bucket($2, $5, timezone => $3)
    $query$
    USING
        p_availability_datastream_ids,
        p_bucket_interval,
        p_bucket_timezone,
        p_system_time_from,
        p_system_time_to,
        p_availability_max_age;
END;
$$;


--------------------------------------------------------------------------------
-- 2. Maintenance function: completely rebuild a Datastream temporal extent.
--
-- This function is NOT used during normal aggregation jobs.
--
-- The aggregation procedure updates Datastream extents incrementally for
-- performance.
--
-- This function should be used after operations that can shrink an extent,
-- for example:
--
--   - deleting historical Observations
--   - manually moving Observation timestamps
--   - major historical maintenance
--
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sensorthings.rebuild_datastream_extent(
    p_datastream_id bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE sensorthings."Datastream" d
    SET
        "phenomenonTime" = x."phenomenonTime",
        "resultTime"     = x."resultTime",
        "last_foi_id"    = x."last_foi_id"
    FROM (
        SELECT
            p_datastream_id AS datastream_id,

            ------------------------------------------------------------------
            -- Full phenomenonTime extent.
            ------------------------------------------------------------------

            CASE
                WHEN COUNT(o."id") > 0 THEN
                    tstzrange(
                        MIN(o."phenomenonTimeStart"),
                        MAX(
                            COALESCE(
                                o."phenomenonTimeEnd",
                                o."phenomenonTimeStart"
                            )
                        ),
                        '[]'
                    )
                ELSE
                    NULL
            END AS "phenomenonTime",

            ------------------------------------------------------------------
            -- Full resultTime extent.
            ------------------------------------------------------------------

            CASE
                WHEN COUNT(o."resultTime") > 0 THEN
                    tstzrange(
                        MIN(o."resultTime"),
                        MAX(o."resultTime"),
                        '[]'
                    )
                ELSE
                    NULL
            END AS "resultTime",

            ------------------------------------------------------------------
            -- FOI associated with the latest Observation.
            ------------------------------------------------------------------

            (
                ARRAY_AGG(
                    o."featuresofinterest_id"
                    ORDER BY
                        o."phenomenonTimeStart" DESC NULLS LAST,
                        o."id" DESC
                )
                FILTER (WHERE o."id" IS NOT NULL)
            )[1] AS "last_foi_id"

        FROM sensorthings."Observation" o
        WHERE o."datastream_id" = p_datastream_id
    ) x
    WHERE d."id" = x.datastream_id;
END;
$$;


--------------------------------------------------------------------------------
-- 3. Generic Datastream aggregation procedure.
--
--
-- SOURCE / TARGET
-- ---------------------------------------------------------------------------
--
-- One of:
--
--   source_datastream_name
--   source_datastream_id
--
-- and one of:
--
--   target_datastream_name
--   target_datastream_id
--
--
-- AGGREGATION CONFIGURATION
-- ---------------------------------------------------------------------------
--
--   bucket_interval
--       Default: "10 minutes"
--
--   bucket_timezone
--       Default: "Etc/GMT-1" (fixed UTC+01:00)
--
--       Examples:
--           "Etc/GMT-1"     fixed UTC+01:00, without daylight saving time
--           "Europe/Zurich" UTC+01:00/+02:00, with daylight saving time
--
--   aggregation
--       Default: "SUM"
--
--       Supported:
--           SUM
--           SUM_NULL_AS_ONE
--           COUNT
--           AVG
--           MIN
--           MAX
--
--   conversion_factor
--       Default: 1.0
--
--       Applied AFTER aggregation.
--       It is NOT applied to the -999.9 missing-data sentinel.
--
--   min_observations
--       Optional non-negative integer. When configured, a bucket is valid
--       only if at least this many numeric Observations pass the quality
--       filters. When omitted, no minimum-count check is performed.
--
--   expected_observation_interval
--       Optional positive interval describing the nominal source cadence,
--       for example "1 hour". Required by max_consecutive_missing.
--
--   max_consecutive_missing
--       Optional non-negative integer. When configured, a bucket is valid
--       only if no run of missing expected source slots exceeds this value.
--       Leading and trailing missing slots are included. When omitted, no
--       consecutive-missing check is performed.
--
--   availability_max_age
--       Optional. When set, normal job mode only creates buckets whose end is
--       not later than the latest station activity plus this interval.
--
--   availability_datastream_ids
--       Optional JSON array of Datastream IDs that prove station activity.
--       The source Datastream itself is always also considered activity.
--
--   empty_bucket_policy
--       Default: "missing"
--       Supported: "missing", "zero_when_available"
--
--       zero_when_available is intended for event/tick sensors. An empty
--       bucket is zero only if a configured availability Datastream has an
--       Observation close enough to that bucket. Otherwise the bucket is
--       skipped. A bucket containing rejected source Observations remains
--       missing; it is never converted to zero.
--
--   empty_bucket_result_quality
--       Default: 1. Quality assigned to a zero inferred from station activity.
--
--   system_time_incremental
--       Default: false. In normal TimescaleDB job mode, also recompute buckets
--       containing source Observations inserted or updated since the previous
--       successful execution.
--
--   system_time_overlap
--       Default: "5 minutes". Re-read this interval before the preceding job
--       completion boundary. Reprocessing is safe because writes use UPSERT.
--
--
-- NORMAL JOB MODE
-- ---------------------------------------------------------------------------
--
--   buckets_to_recompute
--       Default: 3
--
--   Example:
--
--       now = 10:34
--       interval = 10 minutes
--       buckets_to_recompute = 3
--
--       processed buckets:
--
--           (10:00, 10:10]
--           (10:10, 10:20]
--           (10:20, 10:30]
--
--
-- HISTORICAL / MANUAL MODE
-- ---------------------------------------------------------------------------
--
--   window_start
--   window_end
--
--   If both parameters are specified, buckets_to_recompute is ignored.
--
--   Example:
--
--       window_start = 2026-06-01T00:00:00+02:00
--       window_end   = 2026-06-02T00:00:00+02:00
--
--
-- BOUNDARY MODE
-- ---------------------------------------------------------------------------
--
--   boundary_mode
--
--       right_closed    -> (start, end]
--       left_closed     -> [start, end)
--
--   Default: right_closed
--
--
-- RESULT QUALITY FILTERING
-- ---------------------------------------------------------------------------
--
--   result_quality_key
--
--       Optional.
--
--       Example:
--
--           resultQuality = {"code": 2}
--
--           "result_quality_key": "code"
--
--
--   result_quality_allowed
--
--       Optional list of accepted values.
--
--       Example:
--
--           "result_quality_allowed": [0, 1, 2]
--
--
--   result_quality_min
--
--       Optional inclusive lower bound.
--
--       Example:
--
--           "result_quality_min": 2
--
--       means:
--
--           resultQuality >= 2
--
--
--   result_quality_max
--
--       Optional inclusive upper bound.
--
--       Example:
--
--           "result_quality_max": 5
--
--       means:
--
--           resultQuality <= 5
--
--
--   All configured quality filters are combined with AND.
--
--
-- AGGREGATED RESULT QUALITY
-- ---------------------------------------------------------------------------
--
--   If at least one valid Observation is used:
--
--       resultQuality = MIN(resultQuality)
--
--   If no valid Observation exists in the bucket:
--
--       resultNumber  = -999.9
--       resultQuality = 0
--
--
-- FEATURE OF INTEREST
-- ---------------------------------------------------------------------------
--
--   Resolution priority:
--
--       1. config.featuresofinterest_id
--       2. target Datastream.last_foi_id
--       3. source Datastream.last_foi_id
--       4. latest source Observation.featuresofinterest_id
--
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sensorthings.aggregate_datastream(
    job_id int DEFAULT NULL,
    config jsonb DEFAULT '{}'::jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    --------------------------------------------------------------------------
    -- Datastream configuration.
    --------------------------------------------------------------------------

    v_source_datastream_id bigint;
    v_target_datastream_id bigint;

    v_source_datastream_name text;
    v_target_datastream_name text;

    --------------------------------------------------------------------------
    -- Aggregation configuration.
    --------------------------------------------------------------------------

    v_bucket_interval interval;
    v_bucket_timezone text;
    v_buckets_to_recompute int;
    v_conversion_factor double precision;
    v_aggregation text;
    v_boundary_mode text;

    --------------------------------------------------------------------------
    -- Optional completeness validation.
    --------------------------------------------------------------------------

    v_min_observations int;
    v_expected_observation_interval interval;
    v_max_consecutive_missing int;

    --------------------------------------------------------------------------
    -- Availability configuration.
    --------------------------------------------------------------------------

    v_availability_max_age interval;
    v_availability_datastream_ids bigint[] := ARRAY[]::bigint[];
    v_empty_bucket_policy text;
    v_empty_bucket_result_quality double precision;
    v_latest_activity_time timestamptz;
    v_available_until timestamptz;

    --------------------------------------------------------------------------
    -- Incremental system-time configuration.
    --------------------------------------------------------------------------

    v_system_time_incremental boolean;
    v_system_time_overlap interval;
    v_system_time_from timestamptz;
    v_system_time_to timestamptz;
    v_job_id int;

    --------------------------------------------------------------------------
    -- Quality configuration.
    --------------------------------------------------------------------------

    v_quality_key text;
    v_quality_allowed jsonb;
    v_quality_min double precision;
    v_quality_max double precision;

    --------------------------------------------------------------------------
    -- Time window configuration.
    --------------------------------------------------------------------------

    v_config_window_start timestamptz;
    v_config_window_end timestamptz;

    v_series_start timestamptz;
    v_series_end timestamptz;
    v_window_end timestamptz;

    --------------------------------------------------------------------------
    -- Observation configuration.
    --------------------------------------------------------------------------

    v_featuresofinterest_id bigint;
    v_result_type int := 0;

BEGIN

    config := COALESCE(config, '{}'::jsonb);
    v_job_id := job_id;
    v_system_time_to := clock_timestamp();


    --------------------------------------------------------------------------
    -- Resolve source Datastream.
    --------------------------------------------------------------------------

    v_source_datastream_name :=
        NULLIF(config->>'source_datastream_name', '');

    IF config ? 'source_datastream_id' THEN

        v_source_datastream_id :=
            NULLIF(config->>'source_datastream_id', '')::bigint;

    ELSE

        SELECT d."id"
        INTO v_source_datastream_id
        FROM sensorthings."Datastream" d
        WHERE d."name" = v_source_datastream_name
        ORDER BY d."id"
        LIMIT 1;

    END IF;


    --------------------------------------------------------------------------
    -- Resolve target Datastream.
    --------------------------------------------------------------------------

    v_target_datastream_name :=
        NULLIF(config->>'target_datastream_name', '');

    IF config ? 'target_datastream_id' THEN

        v_target_datastream_id :=
            NULLIF(config->>'target_datastream_id', '')::bigint;

    ELSE

        SELECT d."id"
        INTO v_target_datastream_id
        FROM sensorthings."Datastream" d
        WHERE d."name" = v_target_datastream_name
        ORDER BY d."id"
        LIMIT 1;

    END IF;


    --------------------------------------------------------------------------
    -- Validate Datastreams.
    --------------------------------------------------------------------------

    IF v_source_datastream_id IS NULL THEN
        RAISE EXCEPTION
            'Source Datastream not found. Config: %',
            config;
    END IF;

    IF v_target_datastream_id IS NULL THEN
        RAISE EXCEPTION
            'Target Datastream not found. Config: %',
            config;
    END IF;

    IF v_source_datastream_id = v_target_datastream_id THEN
        RAISE EXCEPTION
            'Source and target Datastream must be different. Datastream id: %',
            v_source_datastream_id;
    END IF;


    --------------------------------------------------------------------------
    -- Read aggregation interval.
    --------------------------------------------------------------------------

    v_bucket_interval := COALESCE(
        NULLIF(config->>'bucket_interval', '')::interval,
        interval '10 minutes'
    );

    IF v_bucket_interval <= interval '0 seconds' THEN
        RAISE EXCEPTION
            'bucket_interval must be positive. Got: %',
            v_bucket_interval;
    END IF;


    --------------------------------------------------------------------------
    -- Timezone used to align bucket boundaries.
    --
    -- Etc/GMT-1 is the IANA name for a fixed UTC+01:00 offset. The sign in
    -- the Etc/GMT names is intentionally reversed by IANA convention.
    --------------------------------------------------------------------------

    v_bucket_timezone := COALESCE(
        NULLIF(config->>'bucket_timezone', ''),
        'Etc/GMT-1'
    );


    --------------------------------------------------------------------------
    -- Number of recent buckets to recompute during normal job execution.
    --------------------------------------------------------------------------

    v_buckets_to_recompute := COALESCE(
        NULLIF(config->>'buckets_to_recompute', '')::int,
        3
    );

    IF v_buckets_to_recompute < 1 THEN
        RAISE EXCEPTION
            'buckets_to_recompute must be >= 1. Got: %',
            v_buckets_to_recompute;
    END IF;


    --------------------------------------------------------------------------
    -- Conversion factor.
    --------------------------------------------------------------------------

    v_conversion_factor := COALESCE(
        NULLIF(config->>'conversion_factor', '')::double precision,
        1.0
    );


    --------------------------------------------------------------------------
    -- Aggregation function.
    --------------------------------------------------------------------------

    v_aggregation := upper(
        COALESCE(
            NULLIF(config->>'aggregation', ''),
            'SUM'
        )
    );

    IF v_aggregation NOT IN (
        'SUM',
        'SUM_NULL_AS_ONE',
        'COUNT',
        'AVG',
        'MIN',
        'MAX'
    ) THEN
        RAISE EXCEPTION
            'Unsupported aggregation: %. Allowed: SUM, SUM_NULL_AS_ONE, COUNT, AVG, MIN, MAX',
            v_aggregation;
    END IF;


    --------------------------------------------------------------------------
    -- Optional completeness validation. Existing jobs omit these settings and
    -- therefore retain their previous behaviour.
    --------------------------------------------------------------------------

    IF config ? 'min_observations' THEN
        v_min_observations :=
            NULLIF(config->>'min_observations', '')::int;
    ELSE
        v_min_observations := NULL;
    END IF;

    IF v_min_observations IS NOT NULL AND v_min_observations < 0 THEN
        RAISE EXCEPTION
            'min_observations must be >= 0. Got: %',
            v_min_observations;
    END IF;

    IF config ? 'expected_observation_interval' THEN
        v_expected_observation_interval :=
            NULLIF(config->>'expected_observation_interval', '')::interval;
    ELSE
        v_expected_observation_interval := NULL;
    END IF;

    IF v_expected_observation_interval IS NOT NULL
       AND v_expected_observation_interval <= interval '0 seconds' THEN
        RAISE EXCEPTION
            'expected_observation_interval must be positive. Got: %',
            v_expected_observation_interval;
    END IF;

    IF v_expected_observation_interval IS NOT NULL
       AND v_expected_observation_interval > v_bucket_interval THEN
        RAISE EXCEPTION
            'expected_observation_interval (%) cannot exceed bucket_interval (%)',
            v_expected_observation_interval,
            v_bucket_interval;
    END IF;

    IF config ? 'max_consecutive_missing' THEN
        v_max_consecutive_missing :=
            NULLIF(config->>'max_consecutive_missing', '')::int;
    ELSE
        v_max_consecutive_missing := NULL;
    END IF;

    IF v_max_consecutive_missing IS NOT NULL
       AND v_max_consecutive_missing < 0 THEN
        RAISE EXCEPTION
            'max_consecutive_missing must be >= 0. Got: %',
            v_max_consecutive_missing;
    END IF;

    IF v_max_consecutive_missing IS NOT NULL
       AND v_expected_observation_interval IS NULL THEN
        RAISE EXCEPTION
            'expected_observation_interval is required when max_consecutive_missing is configured';
    END IF;


    --------------------------------------------------------------------------
    -- Boundary mode.
    --------------------------------------------------------------------------

    v_boundary_mode := lower(
        COALESCE(
            NULLIF(config->>'boundary_mode', ''),
            'right_closed'
        )
    );

    IF v_boundary_mode NOT IN (
        'right_closed',
        'left_closed'
    ) THEN
        RAISE EXCEPTION
            'Unsupported boundary_mode: %. Allowed: right_closed, left_closed',
            v_boundary_mode;
    END IF;


    --------------------------------------------------------------------------
    -- Availability and empty-bucket policy.
    --------------------------------------------------------------------------

    IF config ? 'availability_max_age' THEN

        v_availability_max_age :=
            NULLIF(config->>'availability_max_age', '')::interval;

    ELSE

        v_availability_max_age := NULL;

    END IF;

    IF v_availability_max_age IS NOT NULL
       AND v_availability_max_age <= interval '0 seconds' THEN

        RAISE EXCEPTION
            'availability_max_age must be positive. Got: %',
            v_availability_max_age;

    END IF;


    IF config ? 'availability_datastream_ids' THEN

        IF jsonb_typeof(config->'availability_datastream_ids') <> 'array' THEN

            RAISE EXCEPTION
                'availability_datastream_ids must be a JSON array. Got: %',
                config->'availability_datastream_ids';

        END IF;

        SELECT COALESCE(
            ARRAY_AGG(DISTINCT x.value::bigint ORDER BY x.value::bigint),
            ARRAY[]::bigint[]
        )
        INTO v_availability_datastream_ids
        FROM jsonb_array_elements_text(
            config->'availability_datastream_ids'
        ) x(value);

    END IF;


    IF cardinality(v_availability_datastream_ids) > 0 THEN

        IF v_availability_max_age IS NULL THEN

            RAISE EXCEPTION
                'availability_datastream_ids requires availability_max_age';

        END IF;

        IF EXISTS (
            SELECT 1
            FROM unnest(v_availability_datastream_ids) a(datastream_id)
            LEFT JOIN sensorthings."Datastream" d
                ON d."id" = a.datastream_id
            WHERE d."id" IS NULL
        ) THEN

            RAISE EXCEPTION
                'One or more availability Datastreams do not exist. IDs: %',
                v_availability_datastream_ids;

        END IF;

        IF v_target_datastream_id = ANY(v_availability_datastream_ids) THEN

            RAISE EXCEPTION
                'The target Datastream cannot be an availability Datastream. Datastream id: %',
                v_target_datastream_id;

        END IF;

    END IF;


    v_empty_bucket_policy := lower(
        COALESCE(
            NULLIF(config->>'empty_bucket_policy', ''),
            'missing'
        )
    );

    IF v_empty_bucket_policy NOT IN (
        'missing',
        'zero_when_available'
    ) THEN

        RAISE EXCEPTION
            'Unsupported empty_bucket_policy: %. Allowed: missing, zero_when_available',
            v_empty_bucket_policy;

    END IF;

    v_empty_bucket_result_quality := COALESCE(
        NULLIF(config->>'empty_bucket_result_quality', '')::double precision,
        1.0
    );

    IF v_empty_bucket_policy = 'zero_when_available'
       AND (
           v_availability_max_age IS NULL
           OR cardinality(v_availability_datastream_ids) = 0
       ) THEN

        RAISE EXCEPTION
            'empty_bucket_policy=zero_when_available requires availability_max_age and availability_datastream_ids';

    END IF;


    --------------------------------------------------------------------------
    -- Incremental system-time recomputation.
    --------------------------------------------------------------------------

    v_system_time_incremental := COALESCE(
        NULLIF(config->>'system_time_incremental', '')::boolean,
        false
    );

    v_system_time_overlap := COALESCE(
        NULLIF(config->>'system_time_overlap', '')::interval,
        interval '5 minutes'
    );

    IF v_system_time_overlap < interval '0 seconds' THEN

        RAISE EXCEPTION
            'system_time_overlap must be non-negative. Got: %',
            v_system_time_overlap;

    END IF;

    IF v_system_time_incremental
       AND NOT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'sensorthings'
             AND table_name = 'Observation'
             AND column_name = 'systemTimeValidity'
       ) THEN

        RAISE EXCEPTION
            'system_time_incremental requires Observation.systemTimeValidity';

    END IF;


    --------------------------------------------------------------------------
    -- Read explicit historical window.
    --------------------------------------------------------------------------

    IF config ? 'window_start' THEN

        v_config_window_start :=
            NULLIF(config->>'window_start', '')::timestamptz;

    ELSE

        v_config_window_start := NULL;

    END IF;


    IF config ? 'window_end' THEN

        v_config_window_end :=
            NULLIF(config->>'window_end', '')::timestamptz;

    ELSE

        v_config_window_end := NULL;

    END IF;


    --------------------------------------------------------------------------
    -- window_start and window_end must always be provided together.
    --------------------------------------------------------------------------

    IF (
        v_config_window_start IS NULL
        AND v_config_window_end IS NOT NULL
    )
    OR (
        v_config_window_start IS NOT NULL
        AND v_config_window_end IS NULL
    ) THEN

        RAISE EXCEPTION
            'window_start and window_end must be provided together';

    END IF;


    --------------------------------------------------------------------------
    -- Validate explicit historical window.
    --------------------------------------------------------------------------

    IF v_config_window_start IS NOT NULL
       AND v_config_window_start >= v_config_window_end THEN

        RAISE EXCEPTION
            'window_start must be before window_end. Got window_start=% window_end=%',
            v_config_window_start,
            v_config_window_end;

    END IF;


    --------------------------------------------------------------------------
    -- Resolve the forward availability boundary.
    --
    -- The source itself proves activity when it has data. Configured
    -- availability Datastreams provide the same proof for sparse event/tick
    -- sources that legitimately remain empty while the station is active.
    --------------------------------------------------------------------------

    IF v_availability_max_age IS NOT NULL THEN

        SELECT MAX(upper(d."phenomenonTime"))
        INTO v_latest_activity_time
        FROM sensorthings."Datastream" d
        WHERE d."id" = v_source_datastream_id
           OR d."id" = ANY(v_availability_datastream_ids);

        IF v_latest_activity_time IS NOT NULL THEN

            v_available_until :=
                v_latest_activity_time + v_availability_max_age;

        ELSE

            v_available_until := NULL;

        END IF;

    ELSE

        v_latest_activity_time := NULL;
        v_available_until := NULL;

    END IF;


    --------------------------------------------------------------------------
    -- Previous successful job completion used as the incremental change
    -- cursor. Explicit historical/manual calls deliberately do not add
    -- system-time buckets outside their requested window.
    --------------------------------------------------------------------------

    v_system_time_from := NULL;

    IF v_system_time_incremental
       AND v_job_id IS NOT NULL
       AND v_config_window_start IS NULL THEN

        SELECT js.last_successful_finish - v_system_time_overlap
        INTO v_system_time_from
        FROM timescaledb_information.job_stats js
        WHERE js.job_id = v_job_id;

    END IF;


    --------------------------------------------------------------------------
    -- Read resultQuality configuration.
    --------------------------------------------------------------------------

    v_quality_key :=
        NULLIF(config->>'result_quality_key', '');


    --------------------------------------------------------------------------
    -- Allowed resultQuality values.
    --------------------------------------------------------------------------

    IF config ? 'result_quality_allowed' THEN

        v_quality_allowed :=
            config->'result_quality_allowed';

        IF jsonb_typeof(v_quality_allowed) <> 'array' THEN

            RAISE EXCEPTION
                'result_quality_allowed must be a JSON array. Got: %',
                v_quality_allowed;

        END IF;

    ELSE

        v_quality_allowed := NULL;

    END IF;


    --------------------------------------------------------------------------
    -- Minimum accepted resultQuality.
    --
    -- Inclusive:
    --
    --     quality >= result_quality_min
    --------------------------------------------------------------------------

    IF config ? 'result_quality_min' THEN

        v_quality_min :=
            NULLIF(config->>'result_quality_min', '')::double precision;

    ELSE

        v_quality_min := NULL;

    END IF;


    --------------------------------------------------------------------------
    -- Maximum accepted resultQuality.
    --
    -- Inclusive:
    --
    --     quality <= result_quality_max
    --------------------------------------------------------------------------

    IF config ? 'result_quality_max' THEN

        v_quality_max :=
            NULLIF(config->>'result_quality_max', '')::double precision;

    ELSE

        v_quality_max := NULL;

    END IF;


    --------------------------------------------------------------------------
    -- Validate quality range.
    --------------------------------------------------------------------------

    IF v_quality_min IS NOT NULL
       AND v_quality_max IS NOT NULL
       AND v_quality_min > v_quality_max THEN

        RAISE EXCEPTION
            'result_quality_min (%) cannot be greater than result_quality_max (%)',
            v_quality_min,
            v_quality_max;

    END IF;


    --------------------------------------------------------------------------
    -- Resolve FeatureOfInterest.
    --
    -- Observation.featuresofinterest_id is NOT NULL.
    --------------------------------------------------------------------------

    IF config ? 'featuresofinterest_id' THEN

        v_featuresofinterest_id :=
            NULLIF(config->>'featuresofinterest_id', '')::bigint;

    ELSE

        ----------------------------------------------------------------------
        -- First try target.last_foi_id, then source.last_foi_id.
        ----------------------------------------------------------------------

        SELECT
            COALESCE(
                dt."last_foi_id",
                ds."last_foi_id"
            )
        INTO v_featuresofinterest_id
        FROM sensorthings."Datastream" ds
        CROSS JOIN sensorthings."Datastream" dt
        WHERE ds."id" = v_source_datastream_id
          AND dt."id" = v_target_datastream_id;


        ----------------------------------------------------------------------
        -- Otherwise use the FOI from the latest source Observation.
        ----------------------------------------------------------------------

        IF v_featuresofinterest_id IS NULL THEN

            SELECT o."featuresofinterest_id"
            INTO v_featuresofinterest_id
            FROM sensorthings."Observation" o
            WHERE o."datastream_id" = v_source_datastream_id
            ORDER BY
                o."phenomenonTimeStart" DESC,
                o."id" DESC
            LIMIT 1;

        END IF;

    END IF;


    --------------------------------------------------------------------------
    -- No FOI means aggregate Observations cannot be inserted.
    --------------------------------------------------------------------------

    IF v_featuresofinterest_id IS NULL THEN

        RAISE NOTICE
            'No FeatureOfInterest found for source Datastream %. Nothing to aggregate.',
            v_source_datastream_id;

        RETURN;

    END IF;


    --------------------------------------------------------------------------
    -- Aggregated results are numeric.
    --
    -- resultType = 0 corresponds to resultNumber.
    --------------------------------------------------------------------------

    IF config ? 'result_type' THEN

        v_result_type :=
            NULLIF(config->>'result_type', '')::int;

    END IF;


    IF v_result_type <> 0 THEN

        RAISE NOTICE
            'Configured result_type is %, but aggregate values are written to resultNumber.',
            v_result_type;

    END IF;


    --------------------------------------------------------------------------
    -- Determine the bucket series.
    --
    -- EXPLICIT WINDOW MODE:
    --
    --     window_start/window_end are provided.
    --
    -- NORMAL JOB MODE:
    --
    --     recompute the last N closed buckets.
    --------------------------------------------------------------------------

    IF v_config_window_start IS NOT NULL THEN

        ----------------------------------------------------------------------
        -- Align the beginning to a bucket boundary.
        ----------------------------------------------------------------------

        v_series_start :=
            time_bucket(
                v_bucket_interval,
                v_config_window_start,
                timezone => v_bucket_timezone
            );


        ----------------------------------------------------------------------
        -- Align the end to a bucket boundary.
        ----------------------------------------------------------------------

        v_window_end :=
            time_bucket(
                v_bucket_interval,
                v_config_window_end,
                timezone => v_bucket_timezone
            );


        ----------------------------------------------------------------------
        -- If window_end falls inside a bucket, include the complete bucket.
        ----------------------------------------------------------------------

        IF v_window_end < v_config_window_end THEN

            v_window_end :=
                v_window_end + v_bucket_interval;

        END IF;

    ELSE

        ----------------------------------------------------------------------
        -- Only completed buckets are processed.
        ----------------------------------------------------------------------

        v_window_end :=
            time_bucket(
                v_bucket_interval,
                now(),
                timezone => v_bucket_timezone
            );

        v_series_start :=
            v_window_end
            - (v_bucket_interval * v_buckets_to_recompute);

    END IF;


    --------------------------------------------------------------------------
    -- Last bucket start.
    --------------------------------------------------------------------------

    v_series_end :=
        v_window_end - v_bucket_interval;


    --------------------------------------------------------------------------
    -- Perform aggregation.
    --------------------------------------------------------------------------

    WITH normal_buckets AS (

        ----------------------------------------------------------------------
        -- Generate the requested recent or explicit-window buckets. In normal
        -- job mode, stop at the latest known station activity plus the allowed
        -- age. Explicit historical windows remain controlled by their caller.
        ----------------------------------------------------------------------

        SELECT
            s.bucket_start

        FROM generate_series(
                v_series_start,
                v_series_end,
                v_bucket_interval
        ) s(bucket_start)

        WHERE v_config_window_start IS NOT NULL
           OR v_availability_max_age IS NULL
           OR (
               v_available_until IS NOT NULL
               AND s.bucket_start + v_bucket_interval <= v_available_until
           )

    ),


    changed_buckets AS (

        ----------------------------------------------------------------------
        -- Add any closed historical bucket containing a source Observation
        -- inserted or updated since the preceding successful job execution.
        --
        -- Subtract one microsecond for right-closed buckets so an Observation
        -- exactly on a boundary is mapped to the preceding (start, end]
        -- bucket, matching the aggregation join below.
        ----------------------------------------------------------------------

        SELECT c.bucket_start

        FROM sensorthings._changed_observation_buckets(
            v_source_datastream_id,
            v_bucket_interval,
            v_bucket_timezone,
            v_boundary_mode,
            v_system_time_from,
            v_system_time_to
        ) c

    ),


    changed_availability_buckets AS (

        ----------------------------------------------------------------------
        -- A late availability Observation can turn one or more older empty
        -- event/tick buckets into inferred zeros even though no source
        -- Observation changed in those buckets.
        ----------------------------------------------------------------------

        SELECT c.bucket_start

        FROM sensorthings._changed_availability_buckets(
            v_availability_datastream_ids,
            v_bucket_interval,
            v_bucket_timezone,
            CASE
                WHEN v_empty_bucket_policy = 'zero_when_available'
                    THEN v_system_time_from
                ELSE NULL
            END,
            v_system_time_to,
            v_availability_max_age
        ) c

    ),


    buckets AS (

        ----------------------------------------------------------------------
        -- UNION removes duplicates when a recently changed Observation also
        -- belongs to the normal recomputation window.
        ----------------------------------------------------------------------

        SELECT bucket_start
        FROM normal_buckets

        UNION

        SELECT bucket_start
        FROM changed_buckets

        UNION

        SELECT bucket_start
        FROM changed_availability_buckets

    ),


    source_obs AS (

        ----------------------------------------------------------------------
        -- Associate source Observations with each bucket.
        ----------------------------------------------------------------------

        SELECT
            b.bucket_start,
            b.bucket_start + v_bucket_interval AS bucket_end,

            o."id",
            o."resultNumber",
            o."phenomenonTimeStart",

            sensorthings._quality_value_as_double(
                o."resultQuality",
                v_quality_key
            ) AS quality_value

        FROM buckets b

        LEFT JOIN sensorthings."Observation" o

            ON o."datastream_id" = v_source_datastream_id

           AND (

                --------------------------------------------------------------
                -- (start, end]
                --------------------------------------------------------------

                (
                    v_boundary_mode = 'right_closed'

                    AND o."phenomenonTimeStart" > b.bucket_start

                    AND o."phenomenonTimeStart"
                        <= b.bucket_start + v_bucket_interval
                )

                OR

                --------------------------------------------------------------
                -- [start, end)
                --------------------------------------------------------------

                (
                    v_boundary_mode = 'left_closed'

                    AND o."phenomenonTimeStart" >= b.bucket_start

                    AND o."phenomenonTimeStart"
                        < b.bucket_start + v_bucket_interval
                )

           )

    ),


    source_counts AS (

        ----------------------------------------------------------------------
        -- Count all source Observations before quality filtering. This keeps a
        -- bucket containing rejected values distinct from a genuinely empty
        -- event/tick bucket.
        ----------------------------------------------------------------------

        SELECT
            s.bucket_start,
            COUNT(s."id") AS source_observations_seen

        FROM source_obs s

        GROUP BY s.bucket_start

    ),


    bucket_availability AS (

        ----------------------------------------------------------------------
        -- A reference Observation at or shortly before bucket_end proves that
        -- the station was active for an otherwise empty event/tick bucket.
        -- Future Observations never validate an earlier bucket, which avoids
        -- filling outage buckets when a station later recovers.
        ----------------------------------------------------------------------

        SELECT
            b.bucket_start,

            CASE
                WHEN v_empty_bucket_policy <> 'zero_when_available' THEN
                    false

                ELSE EXISTS (
                    SELECT 1
                    FROM sensorthings."Observation" ao
                    WHERE ao."datastream_id"
                          = ANY(v_availability_datastream_ids)
                      AND ao."phenomenonTimeStart"
                          >= b.bucket_start
                             + v_bucket_interval
                             - v_availability_max_age
                      AND ao."phenomenonTimeStart"
                          <= b.bucket_start + v_bucket_interval
                )
            END AS reference_available

        FROM buckets b

    ),


    filtered_obs AS (

        ----------------------------------------------------------------------
        -- Apply resultQuality filtering.
        ----------------------------------------------------------------------

        SELECT
            s.*

        FROM source_obs s

        WHERE s."id" IS NOT NULL


          --------------------------------------------------------------------
          -- Optional allowed-value list.
          --------------------------------------------------------------------

          AND (

              v_quality_allowed IS NULL

              OR EXISTS (

                  SELECT 1

                  FROM jsonb_array_elements_text(
                      v_quality_allowed
                  ) q(value)

                  WHERE s.quality_value =
                        q.value::double precision
              )

          )


          --------------------------------------------------------------------
          -- Optional inclusive minimum.
          --------------------------------------------------------------------

          AND (

              v_quality_min IS NULL

              OR s.quality_value >= v_quality_min

          )


          --------------------------------------------------------------------
          -- Optional inclusive maximum.
          --------------------------------------------------------------------

          AND (

              v_quality_max IS NULL

              OR s.quality_value <= v_quality_max

          )

    ),


    ordered_numeric_obs AS (

        ----------------------------------------------------------------------
        -- Order the numeric Observations that actually participate in the
        -- aggregate. LAG exposes missing nominal slots without materialising
        -- a generate_series for every bucket.
        ----------------------------------------------------------------------

        SELECT
            f.bucket_start,
            f.bucket_end,
            f."phenomenonTimeStart",
            LAG(f."phenomenonTimeStart") OVER (
                PARTITION BY f.bucket_start
                ORDER BY f."phenomenonTimeStart", f."id"
            ) AS previous_time,
            ROW_NUMBER() OVER (
                PARTITION BY f.bucket_start
                ORDER BY f."phenomenonTimeStart" DESC, f."id" DESC
            ) AS reverse_position

        FROM filtered_obs f

        WHERE f."resultNumber" IS NOT NULL

    ),


    missing_runs AS (

        ----------------------------------------------------------------------
        -- Calculate missing nominal slots before/between valid numeric values.
        -- The formulas mirror the configured bucket boundary convention.
        ----------------------------------------------------------------------

        SELECT
            o.bucket_start,
            GREATEST(
                0,
                ROUND(
                    EXTRACT(EPOCH FROM (
                        CASE
                            WHEN o.previous_time IS NOT NULL THEN
                                o."phenomenonTimeStart" - o.previous_time
                                - v_expected_observation_interval
                            WHEN v_boundary_mode = 'right_closed' THEN
                                o."phenomenonTimeStart" - o.bucket_start
                                - v_expected_observation_interval
                            ELSE
                                o."phenomenonTimeStart" - o.bucket_start
                        END
                    ))
                    / EXTRACT(EPOCH FROM v_expected_observation_interval)
                )::int
            ) AS missing_count

        FROM ordered_numeric_obs o

        WHERE v_expected_observation_interval IS NOT NULL

        UNION ALL

        ----------------------------------------------------------------------
        -- Missing nominal slots after the last valid numeric value.
        ----------------------------------------------------------------------

        SELECT
            o.bucket_start,
            GREATEST(
                0,
                ROUND(
                    EXTRACT(EPOCH FROM (
                        o.bucket_end - o."phenomenonTimeStart"
                    ))
                    / EXTRACT(EPOCH FROM v_expected_observation_interval)
                )::int
                - CASE
                    WHEN v_boundary_mode = 'left_closed' THEN 1
                    ELSE 0
                  END
            ) AS missing_count

        FROM ordered_numeric_obs o

        WHERE v_expected_observation_interval IS NOT NULL
          AND o.reverse_position = 1

    ),


    numeric_counts AS (

        SELECT
            f.bucket_start,
            COUNT(f."resultNumber") AS numeric_values_used

        FROM filtered_obs f

        GROUP BY f.bucket_start

    ),


    gap_statistics AS (

        SELECT
            r.bucket_start,
            MAX(r.missing_count) AS max_consecutive_missing_found

        FROM missing_runs r

        GROUP BY r.bucket_start

    ),


    completeness AS (

        ----------------------------------------------------------------------
        -- Both checks are opt-in. An empty bucket has an all-bucket missing
        -- run when a nominal source cadence is configured.
        ----------------------------------------------------------------------

        SELECT
            b.bucket_start,
            COALESCE(n.numeric_values_used, 0) AS numeric_values_used,
            CASE
                WHEN v_expected_observation_interval IS NULL THEN NULL
                ELSE ROUND(
                    EXTRACT(EPOCH FROM v_bucket_interval)
                    / EXTRACT(EPOCH FROM v_expected_observation_interval)
                )::int
            END AS expected_observations,
            CASE
                WHEN v_expected_observation_interval IS NULL THEN NULL
                ELSE COALESCE(
                    g.max_consecutive_missing_found,
                    ROUND(
                        EXTRACT(EPOCH FROM v_bucket_interval)
                        / EXTRACT(EPOCH FROM v_expected_observation_interval)
                    )::int
                )
            END AS max_consecutive_missing_found,
            (
                (v_min_observations IS NULL
                 OR COALESCE(n.numeric_values_used, 0) >= v_min_observations)
                AND
                (v_max_consecutive_missing IS NULL
                 OR COALESCE(
                     g.max_consecutive_missing_found,
                     ROUND(
                         EXTRACT(EPOCH FROM v_bucket_interval)
                         / EXTRACT(EPOCH FROM v_expected_observation_interval)
                     )::int
                 ) <= v_max_consecutive_missing)
            ) AS completeness_valid

        FROM buckets b

        LEFT JOIN numeric_counts n
            ON n.bucket_start = b.bucket_start

        LEFT JOIN gap_statistics g
            ON g.bucket_start = b.bucket_start

    ),


    aggregated AS (

        ----------------------------------------------------------------------
        -- Aggregate every bucket.
        --
        -- IMPORTANT:
        --
        -- Every bucket is returned, even if no valid source Observation exists.
        --
        -- Missing bucket:
        --
        --     resultNumber  = -999.9
        --     resultQuality = 0
        ----------------------------------------------------------------------

        SELECT

            b.bucket_start,

            b.bucket_start
                + v_bucket_interval AS bucket_end,


            ------------------------------------------------------------------
            -- Aggregate result.
            ------------------------------------------------------------------

            CASE

                --------------------------------------------------------------
                -- Optional completeness validation failed.
                --------------------------------------------------------------

                WHEN NOT c.completeness_valid THEN

                    -999.9::double precision

                --------------------------------------------------------------
                -- Empty event/tick bucket while station activity is proven.
                --------------------------------------------------------------

                WHEN sc.source_observations_seen = 0
                     AND v_empty_bucket_policy = 'zero_when_available'
                     AND ba.reference_available THEN

                    0::double precision

                --------------------------------------------------------------
                -- SUM
                --
                -- At least one numeric value is required.
                --------------------------------------------------------------

                WHEN v_aggregation = 'SUM' THEN

                    CASE

                        WHEN COUNT(f."resultNumber") = 0 THEN
                            -999.9::double precision

                        ELSE
                            SUM(f."resultNumber")
                            * v_conversion_factor

                    END


                --------------------------------------------------------------
                -- SUM_NULL_AS_ONE
                --
                -- NULL resultNumber values are counted as 1.
                --------------------------------------------------------------

                WHEN v_aggregation = 'SUM_NULL_AS_ONE' THEN

                    CASE

                        WHEN COUNT(f."id") = 0 THEN
                            -999.9::double precision

                        ELSE
                            SUM(
                                CASE
                                    WHEN f."resultNumber" IS NULL THEN 1
                                    ELSE f."resultNumber"
                                END
                            )
                            * v_conversion_factor

                    END


                --------------------------------------------------------------
                -- COUNT
                --------------------------------------------------------------

                WHEN v_aggregation = 'COUNT' THEN

                    CASE

                        WHEN COUNT(f."id") = 0 THEN
                            -999.9::double precision

                        ELSE
                            COUNT(f."id")::double precision
                            * v_conversion_factor

                    END


                --------------------------------------------------------------
                -- AVG
                --------------------------------------------------------------

                WHEN v_aggregation = 'AVG' THEN

                    CASE

                        WHEN COUNT(f."resultNumber") = 0 THEN
                            -999.9::double precision

                        ELSE
                            AVG(f."resultNumber")
                            * v_conversion_factor

                    END


                --------------------------------------------------------------
                -- MIN
                --------------------------------------------------------------

                WHEN v_aggregation = 'MIN' THEN

                    CASE

                        WHEN COUNT(f."resultNumber") = 0 THEN
                            -999.9::double precision

                        ELSE
                            MIN(f."resultNumber")
                            * v_conversion_factor

                    END


                --------------------------------------------------------------
                -- MAX
                --------------------------------------------------------------

                WHEN v_aggregation = 'MAX' THEN

                    CASE

                        WHEN COUNT(f."resultNumber") = 0 THEN
                            -999.9::double precision

                        ELSE
                            MAX(f."resultNumber")
                            * v_conversion_factor

                    END

            END AS aggregate_value,


            ------------------------------------------------------------------
            -- Aggregated resultQuality.
            --
            -- No valid Observations:
            --
            --     quality = 0
            --
            -- Otherwise:
            --
            --     quality = MIN(source quality)
            ------------------------------------------------------------------

            CASE

                WHEN NOT c.completeness_valid THEN

                    0::double precision

                WHEN sc.source_observations_seen = 0
                     AND v_empty_bucket_policy = 'zero_when_available'
                     AND ba.reference_available THEN

                    v_empty_bucket_result_quality

                WHEN COUNT(f."id") = 0 THEN
                    0::double precision

                ELSE
                    MIN(f.quality_value)

            END AS aggregate_quality_value,


            ------------------------------------------------------------------
            -- Number of source Observations passing the quality filters.
            ------------------------------------------------------------------

            COUNT(f."id") AS observations_used,


            ------------------------------------------------------------------
            -- Number of actual numeric resultNumber values.
            ------------------------------------------------------------------

            COUNT(f."resultNumber") AS numeric_values_used,


            ------------------------------------------------------------------
            -- Optional completeness diagnostics.
            ------------------------------------------------------------------

            c.expected_observations,

            c.max_consecutive_missing_found,

            c.completeness_valid,


            ------------------------------------------------------------------
            -- Empty-bucket and availability diagnostics.
            ------------------------------------------------------------------

            sc.source_observations_seen,

            ba.reference_available,

            (
                sc.source_observations_seen = 0
                AND v_empty_bucket_policy = 'zero_when_available'
                AND ba.reference_available
            ) AS empty_bucket_filled


        FROM buckets b

        LEFT JOIN filtered_obs f
            ON f.bucket_start = b.bucket_start

        JOIN source_counts sc
            ON sc.bucket_start = b.bucket_start

        JOIN bucket_availability ba
            ON ba.bucket_start = b.bucket_start

        JOIN completeness c
            ON c.bucket_start = b.bucket_start

        GROUP BY
            b.bucket_start,
            sc.source_observations_seen,
            ba.reference_available,
            c.expected_observations,
            c.max_consecutive_missing_found,
            c.completeness_valid

    ),


    upserted AS (

        ----------------------------------------------------------------------
        -- Insert or update every aggregate Observation.
        --
        -- Historical recalculation therefore overwrites every bucket in the
        -- requested window.
        ----------------------------------------------------------------------

        INSERT INTO sensorthings."Observation" (

            "phenomenonTimeStart",
            "phenomenonTimeEnd",

            "resultTime",
            "resultType",

            "resultString",
            "resultNumber",
            "resultBoolean",
            "resultJSON",

            "resultQuality",
            "parameters",

            "datastream_id",
            "featuresofinterest_id"

        )

        SELECT

            a.bucket_start,
            a.bucket_end,

            now(),
            v_result_type,

            NULL,
            a.aggregate_value,
            NULL,
            NULL,


            ------------------------------------------------------------------
            -- Preserve the configured resultQuality shape.
            --
            -- Scalar source quality:
            --
            --     0
            --
            -- Object source quality with result_quality_key = "code":
            --
            --     {"code": 0}
            ------------------------------------------------------------------

            CASE

                WHEN v_quality_key IS NOT NULL THEN

                    jsonb_build_object(
                        v_quality_key,
                        COALESCE(
                            a.aggregate_quality_value,
                            0
                        )
                    )

                ELSE

                    to_jsonb(
                        COALESCE(
                            a.aggregate_quality_value,
                            0
                        )
                    )

            END AS "resultQuality",


            ------------------------------------------------------------------
            -- Store aggregation metadata.
            ------------------------------------------------------------------

            jsonb_build_object(

                'aggregation',
                    v_aggregation,

                'source_datastream_id',
                    v_source_datastream_id,

                'source_datastream_name',
                    v_source_datastream_name,

                'target_datastream_id',
                    v_target_datastream_id,

                'target_datastream_name',
                    v_target_datastream_name,

                'bucket_interval',
                    v_bucket_interval::text,

                'bucket_timezone',
                    v_bucket_timezone,

                'buckets_to_recompute',
                    v_buckets_to_recompute,

                'window_start',
                    v_config_window_start,

                'window_end',
                    v_config_window_end,

                'conversion_factor',
                    v_conversion_factor,

                'min_observations',
                    v_min_observations,

                'expected_observation_interval',
                    v_expected_observation_interval,

                'max_consecutive_missing',
                    v_max_consecutive_missing,

                'boundary_mode',
                    v_boundary_mode,

                'result_quality_key',
                    v_quality_key,

                'result_quality_allowed',
                    v_quality_allowed,

                'result_quality_min',
                    v_quality_min,

                'result_quality_max',
                    v_quality_max,

                'availability_max_age',
                    v_availability_max_age,

                'availability_datastream_ids',
                    to_jsonb(v_availability_datastream_ids),

                'latest_activity_time',
                    v_latest_activity_time,

                'available_until',
                    v_available_until,

                'empty_bucket_policy',
                    v_empty_bucket_policy,

                'empty_bucket_result_quality',
                    v_empty_bucket_result_quality,

                'observations_used',
                    a.observations_used,

                'numeric_values_used',
                    a.numeric_values_used,

                'expected_observations',
                    a.expected_observations,

                'missing_observations',
                    CASE
                        WHEN a.expected_observations IS NULL THEN NULL
                        ELSE GREATEST(
                            0,
                            a.expected_observations - a.numeric_values_used
                        )
                    END,

                'max_consecutive_missing_found',
                    a.max_consecutive_missing_found,

                'completeness_valid',
                    a.completeness_valid,

                'completeness_failure',
                    CASE
                        WHEN a.completeness_valid THEN '[]'::jsonb
                        ELSE
                            CASE
                                WHEN v_min_observations IS NOT NULL
                                 AND a.numeric_values_used < v_min_observations
                                    THEN jsonb_build_array('min_observations')
                                ELSE '[]'::jsonb
                            END
                            ||
                            CASE
                                WHEN v_max_consecutive_missing IS NOT NULL
                                 AND a.max_consecutive_missing_found
                                     > v_max_consecutive_missing
                                    THEN jsonb_build_array(
                                        'max_consecutive_missing'
                                    )
                                ELSE '[]'::jsonb
                            END
                    END,

                'source_observations_seen',
                    a.source_observations_seen,

                'reference_available',
                    a.reference_available,

                'empty_bucket_filled',
                    a.empty_bucket_filled,

                'system_time_incremental',
                    v_system_time_incremental,

                'system_time_from',
                    v_system_time_from,

                'system_time_to',
                    v_system_time_to,

                'job_id',
                    job_id

            ) AS "parameters",


            v_target_datastream_id,
            v_featuresofinterest_id


        FROM aggregated a


        ----------------------------------------------------------------------
        -- IMPORTANT:
        --
        -- Normal policies write every generated bucket, including missing
        -- buckets (-999.9). zero_when_available skips a genuinely empty bucket
        -- when no reference Observation proves that the station was active.
        -- Buckets containing source Observations are always written, including
        -- when every source Observation is rejected by quality filters.
        ----------------------------------------------------------------------

        WHERE v_empty_bucket_policy <> 'zero_when_available'
           OR a.source_observations_seen > 0
           OR a.reference_available


        ON CONFLICT (
            "phenomenonTimeStart",
            "phenomenonTimeEnd",
            "datastream_id"
        )

        DO UPDATE SET

            "resultTime" =
                EXCLUDED."resultTime",

            "resultType" =
                EXCLUDED."resultType",

            "resultString" =
                NULL,

            "resultNumber" =
                EXCLUDED."resultNumber",

            "resultBoolean" =
                NULL,

            "resultJSON" =
                NULL,

            "resultQuality" =
                EXCLUDED."resultQuality",

            "parameters" =
                EXCLUDED."parameters",

            "featuresofinterest_id" =
                EXCLUDED."featuresofinterest_id"


        RETURNING

            "phenomenonTimeStart",
            "phenomenonTimeEnd",
            "resultTime",
            "featuresofinterest_id"

    ),


    extent AS (

        ----------------------------------------------------------------------
        -- Calculate the extent only from the Observations affected by this run.
        --
        -- This avoids scanning the entire aggregate Datastream every time.
        ----------------------------------------------------------------------

        SELECT

            CASE

                WHEN COUNT(*) > 0 THEN

                    tstzrange(

                        MIN("phenomenonTimeStart"),

                        MAX(
                            COALESCE(
                                "phenomenonTimeEnd",
                                "phenomenonTimeStart"
                            )
                        ),

                        '[]'
                    )

                ELSE
                    NULL

            END AS new_phenomenon_time,


            CASE

                WHEN COUNT("resultTime") > 0 THEN

                    tstzrange(

                        MIN("resultTime"),
                        MAX("resultTime"),

                        '[]'
                    )

                ELSE
                    NULL

            END AS new_result_time,


            (
                ARRAY_AGG(
                    "featuresofinterest_id"
                    ORDER BY "phenomenonTimeStart" DESC
                )
            )[1] AS new_last_foi_id,


            COUNT(*) AS upserted_count


        FROM upserted

    )


    --------------------------------------------------------------------------
    -- Incrementally update target Datastream metadata.
    --------------------------------------------------------------------------

    UPDATE sensorthings."Datastream" d

    SET

        ----------------------------------------------------------------------
        -- phenomenonTime
        ----------------------------------------------------------------------

        "phenomenonTime" =

            CASE

                WHEN extent.upserted_count = 0 THEN
                    d."phenomenonTime"


                WHEN d."phenomenonTime" IS NULL THEN
                    extent.new_phenomenon_time


                ELSE

                    tstzrange(

                        LEAST(
                            lower(d."phenomenonTime"),
                            lower(extent.new_phenomenon_time)
                        ),

                        GREATEST(
                            upper(d."phenomenonTime"),
                            upper(extent.new_phenomenon_time)
                        ),

                        '[]'
                    )

            END,


        ----------------------------------------------------------------------
        -- resultTime
        ----------------------------------------------------------------------

        "resultTime" =

            CASE

                WHEN extent.upserted_count = 0 THEN
                    d."resultTime"


                WHEN extent.new_result_time IS NULL THEN
                    d."resultTime"


                WHEN d."resultTime" IS NULL THEN
                    extent.new_result_time


                ELSE

                    tstzrange(

                        LEAST(
                            lower(d."resultTime"),
                            lower(extent.new_result_time)
                        ),

                        GREATEST(
                            upper(d."resultTime"),
                            upper(extent.new_result_time)
                        ),

                        '[]'
                    )

            END,


        ----------------------------------------------------------------------
        -- last_foi_id
        --
        -- Historical backfills must not replace last_foi_id when they only
        -- touch Observations older than the current Datastream upper bound.
        ----------------------------------------------------------------------

        "last_foi_id" =

            CASE

                WHEN extent.upserted_count = 0 THEN
                    d."last_foi_id"


                WHEN d."phenomenonTime" IS NULL THEN

                    COALESCE(
                        extent.new_last_foi_id,
                        d."last_foi_id"
                    )


                WHEN upper(extent.new_phenomenon_time)
                     >= upper(d."phenomenonTime") THEN

                    COALESCE(
                        extent.new_last_foi_id,
                        d."last_foi_id"
                    )


                ELSE
                    d."last_foi_id"

            END


    FROM extent

    WHERE d."id" = v_target_datastream_id
      AND extent.upserted_count > 0;


    --------------------------------------------------------------------------
    -- Execution information.
    --------------------------------------------------------------------------

    RAISE NOTICE
        'Aggregated source_datastream_id=% target_datastream_id=% aggregation=% interval=% timezone=% series_start=% window_end=% available_until=% system_time_from=% system_time_to=%',
        v_source_datastream_id,
        v_target_datastream_id,
        v_aggregation,
        v_bucket_interval,
        v_bucket_timezone,
        v_series_start,
        v_window_end,
        v_available_until,
        v_system_time_from,
        v_system_time_to;

END;
$$;



--------------------------------------------------------------------------------
-- 4. Standard 10-minute aggregation wrapper.
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sensorthings.aggregate_datastream_10min(
    job_id int DEFAULT NULL,
    config jsonb DEFAULT '{}'::jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL sensorthings.aggregate_datastream(

        job_id,

        config || jsonb_build_object(
            'bucket_interval',
            '10 minutes'
        )

    );

END;
$$;



--------------------------------------------------------------------------------
-- 5. Standard 1-hour aggregation wrapper.
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sensorthings.aggregate_datastream_1hour(
    job_id int DEFAULT NULL,
    config jsonb DEFAULT '{}'::jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL sensorthings.aggregate_datastream(

        job_id,

        config || jsonb_build_object(
            'bucket_interval',
            '1 hour'
        )

    );

END;
$$;



--------------------------------------------------------------------------------
-- 6. Standard 1-day aggregation wrapper.
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sensorthings.aggregate_datastream_1day(
    job_id int DEFAULT NULL,
    config jsonb DEFAULT '{}'::jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL sensorthings.aggregate_datastream(

        job_id,

        config || jsonb_build_object(
            'bucket_interval',
            '1 day'
        )

    );

END;
$$;



--------------------------------------------------------------------------------
-- 7. Standard 1-year aggregation wrapper.
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sensorthings.aggregate_datastream_1year(
    job_id int DEFAULT NULL,
    config jsonb DEFAULT '{}'::jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL sensorthings.aggregate_datastream(

        job_id,

        config || jsonb_build_object(
            'bucket_interval',
            '1 year'
        )

    );

END;
$$;
