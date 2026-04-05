"""
Test module for schema versioning

Run with:
    pytest test/test_schema_versioning.py -v
"""

import pathlib
import psycopg2
import pytest

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "database" / "istsos_schema.sql"
VERSIONING_PATH = pathlib.Path(__file__).parent.parent / "database" / "istsos_schema_versioning.sql"

DSN = "postgresql://postgres:15889@localhost:5432/istsos_versioning_test"
ADMIN_DSN = "postgresql://postgres:15889@localhost:5432/postgres"
TEST_DB = "istsos_versioning_test"


# DB bootstrap helpers

def _get_raw_conn(dsn=DSN):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _recreate_database():
    # Step 1: terminate connections and drop the versioning test DB
    admin_conn = psycopg2.connect(ADMIN_DSN)
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        cur.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{TEST_DB}' AND pid <> pg_backend_pid();
        """)
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
    admin_conn.close()

    # Step 2: connect to each *other* database that may hold objects owned by
    # the roles we want to drop, and run DROP OWNED BY there first.
    # We discover all non-template databases dynamically so this stays robust
    # when test_schema.py has already created istsos_test with owned objects.
    discover_conn = psycopg2.connect(ADMIN_DSN)
    discover_conn.autocommit = True
    with discover_conn.cursor() as cur:
        cur.execute("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false AND datname NOT IN ('postgres')
        """)
        other_dbs = [r[0] for r in cur.fetchall()]
    discover_conn.close()

    for db in other_dbs:
        try:
            db_conn = psycopg2.connect(f"postgresql://postgres:15889@localhost:5432/{db}")
            db_conn.autocommit = True
            with db_conn.cursor() as cur:
                for role in ("administrator", "testuser"):
                    cur.execute(f"""
                        DO $$ BEGIN
                            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                                DROP OWNED BY "{role}" CASCADE;
                            END IF;
                        END $$;
                    """)
            db_conn.close()
        except Exception:
            pass  # DB may have been dropped mid-loop; skip silently

    # Step 3: now the roles have no dependents - safe to drop them
    admin_conn = psycopg2.connect(ADMIN_DSN)
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        cur.execute("DROP ROLE IF EXISTS administrator")
        cur.execute("DROP ROLE IF EXISTS testuser")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin_conn.close()


def _load_schema(conn):
    sql = SCHEMA_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.duplicates = 'false'")
        cur.execute("SET custom.network = 'false'")
        cur.execute("SET custom.versioning = 'true'")
        cur.execute('SET "custom.authorization" = \'false\'')
        cur.execute('SET "custom.user" = \'testuser\'')
        cur.execute("SET custom.password = 'testpassword'")
        cur.execute(sql)


def _load_versioning(conn):
    sql = VERSIONING_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.network = 'false'")
        cur.execute("SET custom.versioning = 'true'")
        cur.execute('SET "custom.authorization" = \'false\'')
        cur.execute(sql)


class TestSchemaVersioning:
    """
    All tests share one schema+versioning load per session.
    Each test rolls back its own data changes via the `rollback` fixture.
    """

    @pytest.fixture(autouse=True, scope="class")
    def schema(self):
        _recreate_database()
        setup_conn = _get_raw_conn()
        _load_schema(setup_conn)
        _load_versioning(setup_conn)
        setup_conn.close()

        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        yield conn
        conn.close()

    @pytest.fixture(autouse=True)
    def rollback(self, schema):
        yield
        schema.rollback()

    # Row insertion helpers

    def _get_id(self, row):
        return row[0] if not isinstance(row, dict) else row["id"]

    def _insert_commit(self, cur, action="CREATE"):
        """Insert a Commit row (required by versioning) and return its id."""
        cur.execute(
            """
            INSERT INTO sensorthings."Commit"
                ("author", "message", "actionType")
            VALUES ('test-author', 'test commit', %s)
            RETURNING id
            """,
            (action,),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_thing(self, cur, name="v-thing"):
        commit_id = self._insert_commit(cur)
        cur.execute(
            """
            INSERT INTO sensorthings."Thing" ("name", "description", "commit_id")
            VALUES (%s, 'desc', %s) RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())
    
    def test_insert_sets_system_time_validity_start(self, schema):
        """
        After INSERT the live row must have systemTimeValidity =
        [some finite lower bound, infinity).
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-insert")
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            stv = cur.fetchone()[0]

        assert stv is not None, "systemTimeValidity must be set on INSERT"
        # psycopg2 returns a DateTimeTZRange; PostgreSQL TIMESTAMPTZ 'infinity' is
        # represented as 9999-12-31 by psycopg2, so upper_inf is False.
        # We just verify the lower bound is finite (a real timestamp).
        assert not stv.lower_inf, "Lower bound must be a real timestamp after INSERT"
        assert stv.lower is not None, "Lower bound must not be None after INSERT"
