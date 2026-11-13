-- Run as the database owner/superuser after processing-state storage.
-- A privileged, read-only watermark prevents missing late-committing changes.
CREATE OR REPLACE FUNCTION aggregation.safe_change_watermark()
RETURNS TIMESTAMPTZ LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog AS $function$
DECLARE boundary TIMESTAMPTZ;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=current_user AND rolsuper)
        AND NOT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') THEN
        RAISE EXCEPTION 'Watermark function owner needs pg_read_all_stats';
    END IF;
    -- Prepared transactions have no trustworthy start timestamp in this view.
    -- Wait for their resolution rather than advance beyond an unknown change.
    IF EXISTS (SELECT 1 FROM pg_prepared_xacts WHERE database=current_database()) THEN
        RETURN NULL;
    END IF;
    SELECT LEAST(clock_timestamp(), COALESCE(min(xact_start), clock_timestamp()))
    INTO boundary FROM pg_stat_activity
    WHERE datname=current_database() AND pid<>pg_backend_pid();
    -- API as-of query parameters have second precision. Stay strictly behind
    -- the oldest active transaction and overlap subsequent scans by a second.
    RETURN date_trunc('second', boundary) - INTERVAL '1 second';
END;
$function$;
REVOKE ALL ON FUNCTION aggregation.safe_change_watermark() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION aggregation.safe_change_watermark() TO administrator;
COMMENT ON FUNCTION aggregation.safe_change_watermark() IS
    'Stable system-time cutoff before active source transactions; null while prepared transactions exist.';
