# QUICK START

### Create schema

```sql
CREATE SCHEMA IF NOT EXISTS my_schema;
```

### Create tables

```sql
CREATE TABLE IF NOT EXISTS my_schema.users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) UNIQUE NOT NULL,
  email VARCHAR(100) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS my_schema.posts (
  id SERIAL PRIMARY KEY,
  title VARCHAR(100) NOT NULL,
  content TEXT NOT NULL,
  user_id INT REFERENCES my_schema.users(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Add table to versioning

### Create a schema for versioned records named {SCHEMA_NAME}\_history

```sql
CREATE SCHEMA IF NOT EXISTS my_schema_history;
```

### Create trigger to update raw in history in case of insert, update or delete

```sql
CREATE OR REPLACE FUNCTION istsos_mutate_history()
RETURNS trigger
LANGUAGE plpgsql
AS $body$
BEGIN
    IF (TG_OP = 'UPDATE')
    THEN
    -- verify the id is not modified
    IF (NEW.id <> OLD.id)
    THEN
        RAISE EXCEPTION 'the ID must not be changed (%)', NEW.id;
    END IF;
    -- Set the new START system_type_validity for the main table
    NEW.system_time_validity := tstzrange(current_timestamp, TIMESTAMPTZ 'infinity');

    -- Set the END system_time_validity to the 'current_timestamp'
    OLD.system_time_validity := tstzrange(lower(OLD.system_time_validity), current_timestamp);

    -- Copy the original row to the history table
    EXECUTE format('INSERT INTO %I.%I SELECT ($1).\*', TG_TABLE_SCHEMA || '\_history', TG_TABLE_NAME) USING OLD;

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
```

### Create trigger to avoid update and delete in history schema

```sql
CREATE OR REPLACE FUNCTION istsos_prevent_table_update()
RETURNS trigger
LANGUAGE plpgsql
AS $body$
BEGIN
RAISE EXCEPTION 'Updates or Deletes on this table are not allowed';
RETURN NULL;
END;
$body$;
```

### Create function to add table to versioning schema

```sql
CREATE OR REPLACE FUNCTION my_schema.add_table_to_versioning(tablename text, schemaname text DEFAULT 'public')
RETURNS void
LANGUAGE plpgsql
AS $body$
BEGIN
    -- Add the new columns for versioning to the original table
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_time_validity tstzrange DEFAULT tstzrange(current_timestamp, TIMESTAMPTZ ''infinity'');', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commiter text DEFAULT NULL;', schemaname, tablename);
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN system_commit_message text DEFAULT NULL;', schemaname, tablename);

    -- Create a new table with the same structure as the original table, but no data
    EXECUTE format('CREATE TABLE %I.%I AS SELECT \* FROM %I.%I WITH NO DATA;', schemaname || '\_history', tablename, schemaname, tablename);

    -- Add constraint to enforce a single observation does not have two values at the same time
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, system_time_validity WITH &&);', schemaname || '\_history', tablename, tablename || '\_history_unique_obs');

    -- Add triggers for versioning
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE istsos_mutate_history();', tablename || '_history_trigger', schemaname, tablename);

    -- Add triggers to raise an error if the history table is updated or deleted
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION istsos_prevent_table_update();', tablename || '_history_no_mutate', schemaname || '_history', tablename);

    -- Create the traveltime view to query data modification history
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %I.%I UNION SELECT * FROM %I.%I;',
        schemaname, tablename || '_traveltime',
        schemaname, tablename,
        schemaname || '_history', tablename);

    RAISE NOTICE '%.% is now added to versioning', schemaname, tablename;

END;
$body$;
```

### Add the table to the versioning (must respect the order following references)

```sql
SELECT my_schema.add_table_to_versioning('users', 'my_schema');

SELECT my_schema.add_table_to_versioning('posts', 'my_schema');
```

## Insert and update

### Insert data into the users table

```sql
INSERT INTO my_schema.users (username, email) VALUES
('user1', 'user1@example.com'),
('user2', 'user2@example.com'),
('user3', 'user3@example.com');
```

### Insert data into the posts table

```sql
INSERT INTO my_schema.posts (title, content, user_id) VALUES
('First Post', 'Content of the first post.', 1),
('Second Post', 'Content of the second post.', 2),
('Third Post', 'Content of the third post.', 1);
```

### Update data in the users table

```sql
UPDATE my_schema.users
SET email = 'new_email@example.com'
WHERE id = 1;
```

### Update data in the posts table

```sql
UPDATE my_schema.posts
SET content = 'Updated content of the second post'
WHERE id = 2;
```

## Query historical tables

### Get current user with id = 1

```sql
SELECT * FROM my_schema.users
WHERE id = 1;
```

### Get historical changes on user with id = 1

```sql
SELECT * FROM my_schema.users_traveltime
WHERE id = 1;
```

### Get user 1 as it was in the DB at a certain instant in time

```sql
-- note: replace timestamp with correct value
SELECT * FROM my_schema.users_traveltime
WHERE id = 1 and system_time_validity @> timestamptz('2024-04-09T17:16:00Z');
```

### Get user 1 as it was in the DB at a certain period

```sql
-- note: replace timestamps with correct values
SELECT * FROM my_schema.users_traveltime
WHERE id = 1 and system_time_validity && tstzrange(timestamptz('2024-04-09T17:16:00Z'), timestamptz('2024-04-10T19:16:00Z'));
```
