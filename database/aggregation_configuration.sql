-- Configuration only: no aggregation results, worker jobs or source mutations.
-- Apply after the istSOS schema, as the database owner/superuser.
-- Safe to apply again on an existing installation.

CREATE SCHEMA IF NOT EXISTS aggregation AUTHORIZATION administrator;

SET ROLE administrator;

CREATE TABLE IF NOT EXISTS aggregation.datastream_configuration (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    datastream_id BIGINT NOT NULL UNIQUE
        REFERENCES sensorthings."Datastream" (id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    ignore_quality BOOLEAN NOT NULL DEFAULT false,
    mode TEXT NOT NULL DEFAULT 'sampled'
        CHECK (mode IN ('sampled', 'event')),
    min_input_quality SMALLINT NOT NULL
        CHECK (min_input_quality BETWEEN 0 AND 3),
    min_coverage DOUBLE PRECISION
        CHECK (min_coverage BETWEEN 0 AND 1),
    min_observations INTEGER CHECK (min_observations >= 0),
    max_consecutive_missing INTEGER CHECK (max_consecutive_missing >= 0),
    conversion_factor DOUBLE PRECISION
        CHECK (conversion_factor > '-Infinity'::double precision
            AND conversion_factor < 'Infinity'::double precision),
    boundary_mode TEXT CHECK (boundary_mode IN ('right_closed', 'left_closed')),
    utc_offset_minutes SMALLINT CHECK (utc_offset_minutes BETWEEN -840 AND 840),
    buckets_to_recompute INTEGER CHECK (buckets_to_recompute >= 1),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    result_quality_key TEXT CHECK (length(btrim(result_quality_key)) BETWEEN 1 AND 255),
    missing_values DOUBLE PRECISION[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (start_time IS NULL OR end_time IS NULL OR start_time < end_time),
    CHECK (mode = 'sampled' OR (min_coverage IS NULL
        AND min_observations IS NULL AND max_consecutive_missing IS NULL)),
    CHECK (missing_values IS NULL OR (
        COALESCE(array_ndims(missing_values), 1) = 1
        AND array_position(missing_values, NULL) IS NULL
        AND 'Infinity'::double precision > ALL (missing_values)
        AND '-Infinity'::double precision < ALL (missing_values)
    ))
);

-- Existing configurations retain QC filtering when this option is introduced.
ALTER TABLE aggregation.datastream_configuration
    ADD COLUMN IF NOT EXISTS ignore_quality BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN aggregation.datastream_configuration.ignore_quality IS
    'When true, bypass automatic and human QC filtering, including missing quality. '
    'Numeric validity and missing-value rules still apply. Threshold is retained for re-enabling filtering.';

-- Upgrade installations of the initial configuration API: supported intervals
-- are now fixed by the six result hypertables, not per-Datastream settings.
ALTER TABLE aggregation.datastream_configuration DROP COLUMN IF EXISTS intervals;

COMMENT ON TABLE aggregation.datastream_configuration IS
    'Per-source aggregation settings. Does not create jobs or derived Datastreams. '
    'Nullable overrides inherit future worker defaults. QC threshold is required.';

RESET ROLE;

GRANT USAGE ON SCHEMA aggregation TO administrator;
GRANT ALL ON aggregation.datastream_configuration TO administrator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA aggregation TO administrator;
