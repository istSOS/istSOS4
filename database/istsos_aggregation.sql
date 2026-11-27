-- Copyright 2025 SUPSI
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Aggregation service: per-Datastream configuration, result tables, processing state,
-- system-time watermark and change-detection indexes.
-- Apply after istsos_schema.sql, istsos_auth.sql and istsos_schema_versioning.sql, as superuser.

CREATE SCHEMA IF NOT EXISTS aggregation;
GRANT CREATE, USAGE ON SCHEMA aggregation TO "administrator";

SET ROLE "administrator";

-- Per-Datastream settings.
CREATE TABLE IF NOT EXISTS aggregation."datastream_configuration" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "datastream_id" BIGINT NOT NULL REFERENCES sensorthings."Datastream"(id) ON DELETE CASCADE,
    "enabled" BOOLEAN NOT NULL DEFAULT TRUE,
    "ignore_quality" BOOLEAN NOT NULL DEFAULT FALSE,
    "mode" VARCHAR(20) NOT NULL DEFAULT 'sampled' CHECK ("mode" IN ('sampled', 'event')),
    "min_input_quality" INT NOT NULL CHECK ("min_input_quality" BETWEEN 0 AND 3),
    "min_coverage" DOUBLE PRECISION CHECK ("min_coverage" BETWEEN 0 AND 1),
    "min_observations" INT CHECK ("min_observations" >= 0),
    "max_consecutive_missing" INT CHECK ("max_consecutive_missing" >= 0),
    "conversion_factor" DOUBLE PRECISION CHECK ("conversion_factor" > '-Infinity' AND "conversion_factor" < 'Infinity'),
    "boundary_mode" VARCHAR(20) CHECK ("boundary_mode" IN ('right_closed', 'left_closed')),
    "utc_offset_minutes" INT CHECK ("utc_offset_minutes" BETWEEN -840 AND 840),
    -- Look-back for late data when versioning is off (NULL: acquisitionFrequency, else 24 hours).
    "recompute_window" INTERVAL CHECK ("recompute_window" > INTERVAL '0'),
    "start_time" TIMESTAMPTZ DEFAULT NULL,
    "end_time" TIMESTAMPTZ DEFAULT NULL,
    "result_quality_key" VARCHAR(255) CHECK (btrim("result_quality_key") <> ''),
    "missing_values" DOUBLE PRECISION[] DEFAULT NULL,
    "reference_datastream_id" BIGINT REFERENCES sensorthings."Datastream"(id) ON DELETE SET NULL,
    -- An empty event bucket is 0 only with a reference observation this long before its end (NULL: 1 hour).
    "reference_max_age" INTERVAL CHECK ("reference_max_age" > INTERVAL '0'),
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_datastream_configuration_datastream_id UNIQUE ("datastream_id"),
    CONSTRAINT datastream_configuration_period CHECK (
        "start_time" IS NULL OR "end_time" IS NULL OR "start_time" < "end_time"
    ),
    -- Completeness thresholds only make sense for regularly sampled series.
    CONSTRAINT datastream_configuration_event_completeness CHECK (
        "mode" = 'sampled' OR ("min_coverage" IS NULL AND "min_observations" IS NULL AND "max_consecutive_missing" IS NULL)
    ),
    CONSTRAINT datastream_configuration_reference CHECK (
        "reference_datastream_id" IS NULL OR ("mode" = 'event' AND "reference_datastream_id" <> "datastream_id")
    ),
    -- One-dimensional, no NULL elements, finite values.
    CONSTRAINT datastream_configuration_missing_values CHECK (
        "missing_values" IS NULL OR (
            COALESCE(array_ndims("missing_values"), 1) = 1
            AND array_position("missing_values", NULL) IS NULL
            AND 'Infinity' > ALL ("missing_values")
            AND '-Infinity' < ALL ("missing_values")
        )
    )
);

-- Results: one table per interval, one row per Datastream and bucket.
-- 10min, 1hour and 1day come from the observations; 7days, 1month and 1year from the days.
DO $$
DECLARE
    table_ TEXT;
BEGIN
    FOREACH table_ IN ARRAY ARRAY[
        'observations_10min', 'observations_1hour', 'observations_1day',
        'observations_7days', 'observations_1month', 'observations_1year'
    ] LOOP
        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS aggregation.%I (
                "datastream_id" BIGINT NOT NULL REFERENCES sensorthings."Datastream"(id) ON DELETE CASCADE,
                "bucket_start" TIMESTAMPTZ NOT NULL CHECK (isfinite("bucket_start")),
                "bucket_end" TIMESTAMPTZ NOT NULL CHECK (isfinite("bucket_end")),
                "evaluated_until" TIMESTAMPTZ NOT NULL CHECK (isfinite("evaluated_until")),
                "count_total" BIGINT NOT NULL CHECK ("count_total" >= 0),
                "count_accepted" BIGINT NOT NULL,
                "count_numeric" BIGINT NOT NULL,
                "sum_value" DOUBLE PRECISION CHECK ("sum_value" > '-Infinity' AND "sum_value" < 'Infinity'),
                "min_value" DOUBLE PRECISION CHECK ("min_value" > '-Infinity' AND "min_value" < 'Infinity'),
                "max_value" DOUBLE PRECISION CHECK ("max_value" > '-Infinity' AND "max_value" < 'Infinity'),
                "avg_value" DOUBLE PRECISION CHECK ("avg_value" > '-Infinity' AND "avg_value" < 'Infinity'),
                "expected_count" BIGINT CHECK ("expected_count" >= 0),
                "coverage" DOUBLE PRECISION,
                "result_quality" BIGINT NOT NULL CHECK ("result_quality" >= 0),
                "constraint_count" INT NOT NULL CHECK ("constraint_count" BETWEEN 0 AND 28),
                -- Time of each row's write, not of the transaction.
                "calculated_at" TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp() CHECK (isfinite("calculated_at")),
                -- updated_at of the configuration used: results of an older revision are recognisable.
                "configuration_updated_at" TIMESTAMPTZ NOT NULL,
                PRIMARY KEY ("datastream_id", "bucket_start"),
                CONSTRAINT bucket_duration CHECK ("bucket_end" > "bucket_start"),
                CONSTRAINT bucket_evaluated_until CHECK ("evaluated_until" BETWEEN "bucket_start" AND "bucket_end"),
                -- 0 <= numeric <= accepted <= total
                CONSTRAINT bucket_counts CHECK (
                    "count_accepted" BETWEEN 0 AND "count_total"
                    AND "count_numeric" BETWEEN 0 AND "count_accepted"
                ),
                -- No values: all NULL (missing) or all 0 (empty event bucket); otherwise all present.
                CONSTRAINT bucket_statistics CHECK (
                    ("count_numeric" = 0 AND "sum_value" IS NULL AND "min_value" IS NULL
                        AND "max_value" IS NULL AND "avg_value" IS NULL)
                    OR ("count_numeric" = 0 AND "sum_value" = 0 AND "min_value" = 0
                        AND "max_value" = 0 AND "avg_value" = 0)
                    OR ("count_numeric" > 0 AND "sum_value" IS NOT NULL AND "min_value" IS NOT NULL
                        AND "max_value" IS NOT NULL AND "avg_value" IS NOT NULL)
                ),
                CONSTRAINT bucket_min_max CHECK ("min_value" <= "max_value"),
                CONSTRAINT bucket_coverage CHECK (
                    CASE WHEN "expected_count" IS NULL OR "expected_count" = 0
                        THEN "coverage" IS NULL
                        ELSE "coverage" IS NOT NULL AND "coverage" >= 0 AND "coverage" < 'Infinity'
                    END
                ),
                -- Pairs: data + N automatic checks + human + aggregation; N <= 28 fits a BIGINT.
                CONSTRAINT bucket_result_quality CHECK ("result_quality" >> (2 * ("constraint_count" + 3)) = 0)
            )
        $ddl$, table_);
    END LOOP;
END $$;

-- Latest run state of each configuration, written by the worker only (not a history).
CREATE TABLE IF NOT EXISTS aggregation."processing_state" (
    "aggregation_id" BIGINT NOT NULL PRIMARY KEY REFERENCES aggregation."datastream_configuration"(id) ON DELETE CASCADE,
    "status" VARCHAR(20) NOT NULL DEFAULT 'never_run'
        CHECK ("status" IN ('never_run', 'running', 'success', 'failed', 'interrupted')),
    "run_id" UUID DEFAULT NULL,
    "last_started_at" TIMESTAMPTZ DEFAULT NULL CHECK (isfinite("last_started_at")),
    "last_finished_at" TIMESTAMPTZ DEFAULT NULL CHECK (isfinite("last_finished_at")),
    "last_successful_at" TIMESTAMPTZ DEFAULT NULL CHECK (isfinite("last_successful_at")),
    "last_error" jsonb DEFAULT NULL CHECK (jsonb_typeof("last_error") = 'object'),
    "consecutive_failures" BIGINT NOT NULL DEFAULT 0 CHECK ("consecutive_failures" >= 0),
    "checkpoint" jsonb DEFAULT NULL CHECK (jsonb_typeof("checkpoint") = 'object'),
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT processing_state_run_order CHECK ("last_finished_at" >= "last_started_at"),
    -- never_run: no run; running: started, not finished; terminal states: started and finished.
    CONSTRAINT processing_state_status CHECK (
        ("status" = 'never_run' AND "run_id" IS NULL AND "last_started_at" IS NULL AND "last_finished_at" IS NULL)
        OR ("status" = 'running' AND "run_id" IS NOT NULL AND "last_started_at" IS NOT NULL AND "last_finished_at" IS NULL)
        OR ("status" IN ('success', 'failed', 'interrupted') AND "run_id" IS NOT NULL
            AND "last_started_at" IS NOT NULL AND "last_finished_at" IS NOT NULL)
    )
);

-- Every new configuration starts with its state row in never_run.
CREATE OR REPLACE FUNCTION aggregation.initialize_processing_state()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO aggregation."processing_state" ("aggregation_id") VALUES (NEW.id) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER initialize_processing_state
AFTER INSERT ON aggregation."datastream_configuration"
FOR EACH ROW EXECUTE FUNCTION aggregation.initialize_processing_state();

RESET ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA aggregation TO "administrator";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA aggregation TO "administrator";

-- Read access for the roles the API switches to (/Datastreams(n)/Aggregates), when they exist.
DO $$
DECLARE
    role_ TEXT;
BEGIN
    FOREACH role_ IN ARRAY ARRAY['user', 'guest', 'sensor'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA aggregation TO %I', role_);
            EXECUTE format(
                'GRANT SELECT ON aggregation.datastream_configuration, aggregation.observations_10min, '
                'aggregation.observations_1hour, aggregation.observations_1day, aggregation.observations_7days, '
                'aggregation.observations_1month, aggregation.observations_1year TO %I',
                role_
            );
        END IF;
    END LOOP;
END $$;

-- System-time cutoff for the worker: strictly behind the oldest open transaction, so changes
-- committed late are seen by the next run. Created by the superuser (SECURITY DEFINER) because
-- reading other sessions' transaction start in pg_stat_activity needs pg_read_all_stats.
-- Returns NULL while prepared transactions exist (their start time is unknown).
CREATE OR REPLACE FUNCTION aggregation.safe_change_watermark()
RETURNS TIMESTAMPTZ
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    boundary_ TIMESTAMPTZ;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolsuper)
        AND NOT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') THEN
        RAISE EXCEPTION 'Watermark function owner needs pg_read_all_stats';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_prepared_xacts WHERE database = current_database()) THEN
        RETURN NULL;
    END IF;
    SELECT LEAST(clock_timestamp(), COALESCE(min(xact_start), clock_timestamp()))
    INTO boundary_
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid();
    -- $as_of has second precision: truncate, then stay one second behind.
    RETURN date_trunc('second', boundary_) - INTERVAL '1 second';
END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION aggregation.safe_change_watermark() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION aggregation.safe_change_watermark() TO "administrator";

-- Change detection with versioning (only when the versioning schema is installed):
-- current versions opened (inserts, updates) after the last scan.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'sensorthings' AND table_name = 'Observation' AND column_name = 'systemTimeValidity'
    ) THEN
        CREATE INDEX IF NOT EXISTS "idx_observation_datastream_system_time"
        ON sensorthings."Observation" USING btree ("datastream_id" ASC NULLS LAST, lower("systemTimeValidity") ASC NULLS LAST);
    END IF;
END $$;
