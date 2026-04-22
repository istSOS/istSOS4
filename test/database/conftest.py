"""
test/database/conftest.py -- shared fixtures and helpers for the database test suite.

Three test files exercise the three SQL layers of istSOS4:

    test_schema.py          -- istsos_schema.sql (base schema, no auth, no versioning)
    test_schema_versioning.py -- istsos_schema_versioning.sql (system-time versioning)
    test_auth_sql.py        -- istsos_auth.sql (users, commits, RLS, policies)

This conftest provides:

  1. Shared constants
       PG_HOST, PG_PORT, PG_SUPERUSER, PG_PASSWORD, ADMIN_DSN, DB_PREFIX
       Schema SQL paths (SCHEMA_SQL, AUTH_SQL, VERSIONING_SQL)

  2. recreate_database(test_db, roles_to_drop)
       Single function that every test module calls.  Handles:
         - terminating active connections
         - DROP DATABASE
         - DROP OWNED BY <role> CASCADE across the whole cluster (prevents
           DependentObjectsStillExist when test ordering puts auth before schema)
         - DROP ROLE
         - CREATE DATABASE

  3. get_raw_conn(dsn)
       Open a psycopg2 connection with autocommit=True.

  4. make_dsn(db_name)
       Build a connection string from the shared constants.

  5. Schema loaders: load_base_schema, load_auth_schema, load_versioning_schema
       Each accepts an open connection and sets the custom.* GUCs before
       running the SQL, so individual test files do not repeat the SET calls.

  6. Shared SQL insert helpers (module-level functions, not class methods)
       These have the same signatures used by all three test classes:

         get_id(row)
         insert_minimal_location(cur, name, *, commit_id=None)
         insert_minimal_thing(cur, name, *, commit_id=None)
         insert_minimal_sensor(cur, name, *, commit_id=None)
         insert_minimal_observed_property(cur, name, *, commit_id=None)
         insert_minimal_datastream(cur, thing_id, sensor_id, op_id, name, *, commit_id=None)
         insert_minimal_foi(cur, name, *, commit_id=None)

       commit_id is keyword-only and defaults to None so the same helper
       works for the no-auth schema (commit_id omitted) and the auth schema
       (commit_id passed in).

  7. Pytest fixtures
       pg_conn(dsn)    -- yields a transactional psycopg2 connection
       auto_rollback   -- rolls back after each test (used by all three classes)

Usage in a test file:

    from test.database.conftest import (
        recreate_database, get_raw_conn, make_dsn,
        load_base_schema, load_auth_schema,
        insert_minimal_thing, insert_minimal_location,
        ...
    )
"""

import pathlib

import psycopg2
import pytest

# ---------------------------------------------------------------------------
# Shared connection constants
# All three test files connect to the same Postgres instance.
# ---------------------------------------------------------------------------

PG_HOST = "localhost"
PG_PORT = 5432
PG_SUPERUSER = "postgres"
PG_PASSWORD = "15889"

ADMIN_DSN = (
    f"postgresql://{PG_SUPERUSER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres"
)

# Path to the SQL files (resolved relative to this conftest's location).
# conftest lives at  test/database/conftest.py
# SQL files live at  database/*.sql  (repo root / database /)
_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
DATABASE_DIR = _REPO_ROOT / "database"

SCHEMA_SQL = DATABASE_DIR / "istsos_schema.sql"
AUTH_SQL = DATABASE_DIR / "istsos_auth.sql"
VERSIONING_SQL = DATABASE_DIR / "istsos_schema_versioning.sql"

# All roles that any of the three SQL layers can create.
# Used as the default set to clean up during recreate_database.
ALL_SCHEMA_ROLES = ("administrator", "testuser", "user", "guest", "sensor")


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def make_dsn(db_name: str) -> str:
    """Build a libpq connection string for a named database on the shared instance."""
    return (
        f"postgresql://{PG_SUPERUSER}:{PG_PASSWORD}"
        f"@{PG_HOST}:{PG_PORT}/{db_name}"
    )


def get_raw_conn(dsn: str) -> psycopg2.extensions.connection:
    """Open a psycopg2 connection with autocommit enabled (safe for DDL)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


# ---------------------------------------------------------------------------
# Database lifecycle
# ---------------------------------------------------------------------------


def recreate_database(
    test_db: str, roles_to_drop: tuple = ALL_SCHEMA_ROLES
) -> None:
    """
    Drop and recreate *test_db*, cleaning up cluster-level roles beforehand.

    PostgreSQL roles are cluster-level and survive DROP DATABASE.  If a
    previous test run left a role owning objects in another surviving
    database, DROP ROLE raises DependentObjectsStillExist.  We prevent
    that by connecting to every non-template database and issuing
    DROP OWNED BY <role> CASCADE before touching the roles.

    Steps:
      1. Terminate active connections to test_db.
      2. DROP DATABASE IF EXISTS test_db.
      3. For every surviving database: DROP OWNED BY each role (safe no-op
         when the role owns nothing there).
      4. DROP ROLE IF EXISTS for each role.
      5. CREATE DATABASE test_db.
    """
    admin_conn = get_raw_conn(ADMIN_DSN)

    with admin_conn.cursor() as cur:
        # Terminate active connections so DROP DATABASE does not block.
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid()
            """,
            (test_db,),
        )
        cur.execute(f"DROP DATABASE IF EXISTS {test_db}")

        # Collect surviving databases before re-opening connections.
        cur.execute(
            """
            SELECT datname FROM pg_database
            WHERE datistemplate = false
            """,
        )
        surviving_dbs = [r[0] for r in cur.fetchall()]

    admin_conn.close()

    # Drop all owned objects in every surviving database so DROP ROLE can succeed.
    for db in surviving_dbs:
        try:
            db_conn = get_raw_conn(make_dsn(db))
            with db_conn.cursor() as cur:
                for role in roles_to_drop:
                    # DO block avoids an error when the role does not exist yet.
                    cur.execute(
                        f"""
                        DO $$
                        BEGIN
                            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                                REASSIGN OWNED BY "{role}" TO {PG_SUPERUSER};
                                DROP OWNED BY "{role}" CASCADE;
                            END IF;
                        END $$;
                        """
                    )
            db_conn.close()
        except Exception:
            pass  # DB may have been dropped by a concurrent test

    # Drop roles then create the fresh database.
    admin_conn = get_raw_conn(ADMIN_DSN)
    with admin_conn.cursor() as cur:
        for role in roles_to_drop:
            cur.execute(f'DROP ROLE IF EXISTS "{role}"')
        cur.execute(f"CREATE DATABASE {test_db}")
    admin_conn.close()


# ---------------------------------------------------------------------------
# Schema loaders
# Each loader accepts an open autocommit connection and applies the relevant
# SQL file after setting the custom.* GUCs that the SQL expects.
# ---------------------------------------------------------------------------


def load_base_schema(
    conn, *, versioning: bool = False, authorization: bool = False
) -> None:
    """
    Execute istsos_schema.sql on *conn*.

    versioning    -- set custom.versioning = 'true'  (needed by versioning tests)
    authorization -- set custom.authorization = 'true' (needed by auth tests)
    """
    sql = SCHEMA_SQL.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.duplicates = 'false'")
        cur.execute("SET custom.network = 'false'")
        cur.execute(
            f"SET custom.versioning = '{'true' if versioning else 'false'}'"
        )
        cur.execute(
            f"SELECT set_config('custom.authorization', "
            f"'{'true' if authorization else 'false'}', false)"
        )
        cur.execute("SET \"custom.user\" = 'testuser'")
        cur.execute("SET custom.password = 'testpassword'")
        cur.execute(sql)


def load_auth_schema(conn) -> None:
    """Execute istsos_auth.sql on *conn* (must run after load_base_schema)."""
    sql = AUTH_SQL.read_text()
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('custom.authorization', 'true', false)")
        cur.execute("SET custom.network = 'false'")
        cur.execute("SET \"custom.user\" = 'testuser'")
        cur.execute(sql)


def load_versioning_schema(conn) -> None:
    """Execute istsos_schema_versioning.sql on *conn* (must run after load_base_schema)."""
    sql = VERSIONING_SQL.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.network = 'false'")
        cur.execute("SET custom.versioning = 'true'")
        cur.execute(
            "SELECT set_config('custom.authorization', 'false', false)"
        )
        cur.execute(sql)


# ---------------------------------------------------------------------------
# Shared SQL insert helpers
#
# commit_id is keyword-only with a default of None so the same function
# body works for both the no-auth schema (where the column does not exist)
# and the auth/versioning schemas (where it must be supplied).
#
# test_schema.py     -> never passes commit_id
# test_schema_versioning.py -> passes commit_id (inserts its own Commit row)
# test_auth_sql.py   -> passes commit_id (always has a User + Commit chain)
# ---------------------------------------------------------------------------


def get_id(row) -> int:
    """Extract the integer id from a psycopg2 row regardless of cursor factory."""
    return row[0] if not isinstance(row, dict) else row["id"]


def insert_minimal_thing(
    cur, name: str = "test-thing", *, commit_id=None
) -> int:
    """Insert a Thing with the minimum required columns and return its id."""
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."Thing" ("name", "description")
            VALUES (%s, 'desc')
            RETURNING id
            """,
            (name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."Thing" ("name", "description", "commit_id")
            VALUES (%s, 'desc', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
    return get_id(cur.fetchone())


def insert_minimal_location(
    cur, name: str = "test-loc", *, commit_id=None
) -> int:
    """Insert a Location with a fixed point geometry and return its id."""
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."Location"
                ("name", "description", "encodingType", "location")
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326))
            RETURNING id
            """,
            (name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."Location"
                ("name", "description", "encodingType", "location", "commit_id")
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326), %s)
            RETURNING id
            """,
            (name, commit_id),
        )
    return get_id(cur.fetchone())


def insert_minimal_sensor(
    cur, name: str = "test-sensor", *, commit_id=None
) -> int:
    """Insert a Sensor and return its id."""
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."Sensor"
                ("name", "description", "encodingType", "metadata")
            VALUES (%s, 'desc', 'application/pdf', 'http://meta')
            RETURNING id
            """,
            (name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."Sensor"
                ("name", "description", "encodingType", "metadata", "commit_id")
            VALUES (%s, 'desc', 'application/pdf', 'http://meta', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
    return get_id(cur.fetchone())


def insert_minimal_observed_property(
    cur, name: str = "test-op", *, commit_id=None
) -> int:
    """Insert an ObservedProperty and return its id."""
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."ObservedProperty"
                ("name", "definition", "description")
            VALUES (%s, 'http://def', 'desc')
            RETURNING id
            """,
            (name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."ObservedProperty"
                ("name", "definition", "description", "commit_id")
            VALUES (%s, 'http://def', 'desc', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
    return get_id(cur.fetchone())


def insert_minimal_datastream(
    cur,
    thing_id: int,
    sensor_id: int,
    op_id: int,
    name: str = "test-ds",
    *,
    commit_id=None,
) -> int:
    """Insert a Datastream linking the given Thing/Sensor/ObservedProperty and return its id."""
    _UOM = '{"name":"C","symbol":"C","definition":"http://d"}'
    _OBS_TYPE = (
        "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    )
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."Datastream"
                ("name", "description", "unitOfMeasurement",
                 "observationType", "thing_id", "sensor_id", "observedproperty_id")
            VALUES (%s, 'desc', %s::jsonb, %s, %s, %s, %s)
            RETURNING id
            """,
            (name, _UOM, _OBS_TYPE, thing_id, sensor_id, op_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."Datastream"
                ("name", "description", "unitOfMeasurement",
                 "observationType", "thing_id", "sensor_id",
                 "observedproperty_id", "commit_id")
            VALUES (%s, 'desc', %s::jsonb, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (name, _UOM, _OBS_TYPE, thing_id, sensor_id, op_id, commit_id),
        )
    return get_id(cur.fetchone())


def insert_minimal_foi(cur, name: str = "test-foi", *, commit_id=None) -> int:
    """Insert a FeaturesOfInterest with a fixed point geometry and return its id."""
    if commit_id is None:
        cur.execute(
            """
            INSERT INTO sensorthings."FeaturesOfInterest"
                ("name", "description", "encodingType", "feature")
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326))
            RETURNING id
            """,
            (name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO sensorthings."FeaturesOfInterest"
                ("name", "description", "encodingType", "feature", "commit_id")
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326), %s)
            RETURNING id
            """,
            (name, commit_id),
        )
    return get_id(cur.fetchone())


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_conn(request):
    """
    Yield a transactional psycopg2 connection for *dsn*.

    Intended to be called indirectly from a class-scoped fixture that passes
    the correct DSN.  Usage inside a test class:

        @pytest.fixture(autouse=True, scope="class")
        def schema(self):
            recreate_database(TEST_DB)
            setup_conn = get_raw_conn(DSN)
            load_base_schema(setup_conn)
            setup_conn.close()

            conn = psycopg2.connect(DSN)
            conn.autocommit = False
            yield conn
            conn.close()

    The auto_rollback fixture below handles per-test rollback.
    """
    dsn = request.param
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def auto_rollback(request):
    """
    Roll back all mutations after each test automatically.

    Works by locating the 'schema' fixture on the owning class (if present)
    and calling rollback() on it.  This matches the pattern used by all three
    database test classes which store their shared connection as self.schema.
    """
    yield
    # Walk the fixtures to find the class-scoped connection.
    # If the test does not have a 'schema' fixture nothing happens.
    schema_conn = request.node.funcargs.get("schema")
    if schema_conn is not None and hasattr(schema_conn, "rollback"):
        schema_conn.rollback()
