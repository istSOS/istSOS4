-- =======================
-- SYSTEM_TIME extension
-- =======================

-- triggers to handle table versioning with system_time
CREATE OR REPLACE FUNCTION istsos_mutate_history()
RETURNS trigger 
LANGUAGE plpgsql
AS $body$
DECLARE
    commit_id INTEGER;
BEGIN
    IF (TG_OP = 'UPDATE')
    THEN
        -- verify the id is not modified
        IF (NEW.id <> OLD.id)
        THEN
            RAISE EXCEPTION 'the ID must not be changed (%)', NEW.id;
        END IF;

        -- Insert a new record into the Commit table
        EXECUTE format(
            'INSERT INTO sensorthings."Commit" (author, message) VALUES (%L, %L) RETURNING id',
            TG_TABLE_NAME || ' user ' || NEW.id,
            TG_TABLE_NAME || ' commit ' || NEW.id
        )
        INTO commit_id;

        NEW.commit_id := commit_id;

        -- Set the new START system_type_validity for the main table
        NEW.system_time_validity := tstzrange(current_timestamp, TIMESTAMPTZ  'infinity');
        -- Set the END system_time_validity to the 'current_timestamp'
        OLD.system_time_validity := tstzrange(lower(OLD.system_time_validity), current_timestamp);
        -- Copy the original row to the history table
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME) USING OLD;
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'INSERT')
    THEN
        -- Set the new START system_type_validity for the main table
        NEW.system_time_validity := tstzrange(current_timestamp, 'infinity');
        -- Return the NEW record modified to run the table UPDATE
        RETURN NEW;
    END IF;

    IF (TG_OP = 'DELETE')
    THEN
        -- Set the END system_time_validity to the 'current_timestamp'
        OLD.system_time_validity := tstzrange(lower(OLD.system_time_validity), current_timestamp);
        -- Copy the original row to the history table
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA || '_history', TG_TABLE_NAME) USING OLD;
        RETURN OLD;
    END IF;    
END;
$body$;


CREATE OR REPLACE FUNCTION istsos_prevent_table_update()
RETURNS trigger 
LANGUAGE plpgsql
AS $body$
BEGIN
    RAISE EXCEPTION 'Updates or Deletes on this table are not allowed';
    RETURN NULL;
END;
$body$;

-- function to add a table to system_time versioning system
CREATE OR REPLACE FUNCTION sensorthings.add_table_to_versioning(tablename text, schemaname text DEFAULT 'public')
RETURNS void 
LANGUAGE plpgsql
AS $body$
BEGIN
    -- Add the new columns for versioning to the original table
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_time_validity tstzrange DEFAULT tstzrange(current_timestamp, TIMESTAMPTZ ''infinity'');', schemaname, tablename);
    -- EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commiter text DEFAULT NULL;', schemaname, tablename);
    -- EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commit_message text DEFAULT NULL;', schemaname, tablename);

    -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I WITH NO DATA;', schemaname || '_history', tablename, schemaname, tablename);
    -- Add constraint to enforce a single observation does not have two values at the same time
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, system_time_validity WITH &&);', schemaname || '_history', tablename, tablename || '_history_unique_obs');

    -- Add triggers for versioning
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE istsos_mutate_history();', tablename || '_history_trigger', schemaname, tablename);

    -- Add triggers to raise an error if the history table is updated or deleted
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION istsos_prevent_table_update();', tablename || '_history_no_mutate', schemaname || '_history', tablename);

    -- Create the travelitime view to query data modification history
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I UNION SELECT * FROM %I.%I;',
        schemaname, tablename || '_traveltime',
        schemaname, tablename,
        schemaname || '_history', tablename);

    RAISE NOTICE '%.% is now added to versioning', schemaname, tablename;
END;
$body$;

CREATE OR REPLACE FUNCTION sensorthings.add_schema_to_versioning(original_schema text)
RETURNS void
LANGUAGE plpgsql
AS $body$
DECLARE
    tablename text;
BEGIN
    -- Create the history schema if it doesn't exist
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I_history;', original_schema);

    -- Loop through each table in the original schema in the correct order
    FOR tablename IN
        SELECT unnest(array['Location', 'Thing', 'HistoricalLocation', 'ObservedProperty', 'Sensor', 'Datastream', 'FeaturesOfInterest', 'Observation'])
        LOOP
            -- Add versioning to each table
            EXECUTE format('SELECT sensorthings.add_table_to_versioning(%L, %L);', tablename, original_schema);
        END LOOP;

    EXECUTE 'CREATE OR REPLACE FUNCTION result(sensorthings."Observation_traveltime") RETURNS jsonb AS $$ BEGIN RETURN CASE  WHEN $1."resultType" = 0 THEN to_jsonb($1."resultString") WHEN $1."resultType" = 1 THEN to_jsonb($1."resultInteger") WHEN $1."resultType" = 2 THEN to_jsonb($1."resultDouble") WHEN $1."resultType" = 3 THEN to_jsonb($1."resultBoolean") WHEN $1."resultType" = 4 THEN $1."resultJSON" ELSE NULL::jsonb END; END; $$ LANGUAGE plpgsql;';

    RAISE NOTICE 'Schema % is now versionized.', original_schema;
END;
$body$;

DO $$
BEGIN
-- Check if custom versioning is enabled
    IF current_setting('custom.versioning', true)::boolean THEN
    -- First, set up schema versioning
        EXECUTE 'SELECT sensorthings.add_schema_to_versioning(''sensorthings'');';

    END IF;
END $$;
