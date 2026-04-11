"""
test(auth): direct psycopg2 tests for istsos_auth.sql logic

Run with:
    pytest test/test_auth_sql.py -v
"""

import pathlib
import psycopg2
import pytest

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "database" / "istsos_schema.sql"
AUTH_PATH = pathlib.Path(__file__).parent.parent / "database" / "istsos_auth.sql"

DSN = "postgresql://postgres:15889@localhost:5432/istsos_test_auth"
ADMIN_DSN = "postgresql://postgres:15889@localhost:5432/postgres"
TEST_DB = "istsos_test_auth"

def _get_raw_conn():
    """Open a connection with autocommit so DDL runs freely."""
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn


def _recreate_database():
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{TEST_DB}' AND pid <> pg_backend_pid();
        """)

        # Drop and recreate DB
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")

        # Get all databases
        cur.execute("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false
        """)
        dbs = [r[0] for r in cur.fetchall()]

    conn.close()

    for db in dbs:
        try:
            db_conn = psycopg2.connect(f"postgresql://postgres:15889@localhost:5432/{db}")
            db_conn.autocommit = True

            with db_conn.cursor() as cur:
                for role in ("administrator", "testuser", "user", "guest", "sensor"):
                    cur.execute(f"""
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                            REASSIGN OWNED BY "{role}" TO postgres;
                            DROP OWNED BY "{role}" CASCADE;
                        END IF;
                    END
                    $$;
                    """)

            db_conn.close()
        except Exception:
            pass

    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True

    with conn.cursor() as cur:
        for role in ("administrator", "testuser", "user", "guest", "sensor"):
            cur.execute(f'DROP ROLE IF EXISTS "{role}"')

    conn.close()


def _load_schema(conn):
    sql = SCHEMA_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.duplicates = 'false'")
        cur.execute("SET custom.network = 'false'")
        cur.execute('SET "custom.user" = \'testuser\'')
        cur.execute("SET custom.password = 'testpassword'")
        cur.execute(sql)


def _load_auth(conn):
    sql = AUTH_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute('SET "custom.authorization" = \'true\'')
        cur.execute("SET custom.network = 'false'")
        cur.execute('SET "custom.user" = \'testuser\'')
        cur.execute(sql)


class TestAuth:
    """
    All tests share a single schema + auth load per pytest session.
    Each test rolls back its own data changes via the rollback fixture.
    """

    @pytest.fixture(autouse=True, scope="class")
    def schema(self):
        _recreate_database()

        setup_conn = _get_raw_conn()
        _load_schema(setup_conn)
        _load_auth(setup_conn)
        setup_conn.close()

        conn = psycopg2.connect(DSN)
        conn.autocommit = False

        yield conn

        conn.close()

    @pytest.fixture(autouse=True)
    def rollback(self, schema):
        yield
        schema.rollback()

    """
    Helpers
    """

    def _get_id(self, row):
        return row[0] if not isinstance(row, dict) else row["id"]

    def _insert_user(self, cur, username="test-user", role="administrator"):
        cur.execute(
            """
            INSERT INTO sensorthings."User" (username, role)
            VALUES (%s, %s) RETURNING id
            """,
            (username, role),
        )
        return self._get_id(cur.fetchone())

    """
    1. User table
    """

    def test_user_table_exists(self, schema):
        """sensorthings.User table must be created by the auth script."""
        with schema.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'sensorthings' AND table_name = 'User'
                """
            )
            row = cur.fetchone()
        assert row is not None

    def test_user_table_has_required_columns(self, schema):
        """User table must expose id, username, role, contact, uri."""
        with schema.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'sensorthings' AND table_name = 'User'
                """
            )
            cols = {r[0] for r in cur.fetchall()}
        for expected in ("id", "username", "role", "contact", "uri"):
            assert expected in cols, f"User table missing column: {expected}"

    def test_user_username_unique(self, schema):
        """Inserting duplicate usernames must raise UniqueViolation."""
        with schema.cursor() as cur:
            self._insert_user(cur, username="dup-user")
            with pytest.raises(psycopg2.errors.UniqueViolation):
                self._insert_user(cur, username="dup-user")

    def test_user_role_not_nullable(self, schema):
        """Omitting role must raise NotNullViolation."""
        with schema.cursor() as cur:
            with pytest.raises(psycopg2.errors.NotNullViolation):
                cur.execute(
                    "INSERT INTO sensorthings.\"User\" (username) VALUES ('no-role')"
                )

    def test_user_selflink_format(self, schema):
        """@iot.selfLink for User must return '/Users(<id>)'."""
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username="sl-user")
            cur.execute(
                'SELECT "@iot.selfLink"(u) FROM sensorthings."User" u WHERE id = %s',
                (uid,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Users({uid})"
