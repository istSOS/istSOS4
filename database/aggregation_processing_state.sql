-- Latest service-managed state, not run history or SensorThings versioning.
-- Apply after aggregation_configuration.sql. Re-applicable without resets.
SET ROLE administrator;

CREATE TABLE IF NOT EXISTS aggregation.processing_state (
    configuration_id BIGINT PRIMARY KEY
        REFERENCES aggregation.datastream_configuration(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'never_run'
        CHECK (status IN ('never_run', 'running', 'success', 'partial', 'failed', 'interrupted')),
    phase TEXT NOT NULL DEFAULT 'discovery' CHECK (phase IN ('discovery', 'aggregation')),
    run_id UUID,
    last_started_at TIMESTAMPTZ CHECK (isfinite(last_started_at)),
    last_finished_at TIMESTAMPTZ CHECK (isfinite(last_finished_at)),
    last_successful_at TIMESTAMPTZ CHECK (isfinite(last_successful_at)),
    last_error JSONB CHECK (jsonb_typeof(last_error) = 'object'),
    consecutive_failures BIGINT NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    checkpoint JSONB CHECK (jsonb_typeof(checkpoint) = 'object'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (last_finished_at >= last_started_at),
    CHECK (
        (status = 'never_run' AND run_id IS NULL AND last_started_at IS NULL AND last_finished_at IS NULL)
        OR (status = 'running' AND run_id IS NOT NULL AND last_started_at IS NOT NULL AND last_finished_at IS NULL)
        OR (status IN ('success', 'partial', 'failed', 'interrupted') AND run_id IS NOT NULL
            AND last_started_at IS NOT NULL AND last_finished_at IS NOT NULL)
    )
);

CREATE OR REPLACE FUNCTION aggregation.initialize_processing_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
BEGIN
    INSERT INTO aggregation.processing_state(configuration_id) VALUES (NEW.id)
        ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE TRIGGER initialize_processing_state
AFTER INSERT ON aggregation.datastream_configuration
FOR EACH ROW EXECUTE FUNCTION aggregation.initialize_processing_state();

INSERT INTO aggregation.processing_state(configuration_id)
SELECT id FROM aggregation.datastream_configuration ON CONFLICT DO NOTHING;

COMMENT ON TABLE aggregation.processing_state IS
    'One latest state per configuration. Written by the service, exposed read-only through API. '
    'Discovery does not advance the reserved aggregation checkpoint.';
GRANT ALL ON aggregation.processing_state TO administrator;
RESET ROLE;
