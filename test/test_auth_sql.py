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

    def _insert_commit(self, cur, user_id, action="CREATE"):
        cur.execute(
            """
            INSERT INTO sensorthings."Commit"
                (author, message, "actionType", user_id)
            VALUES ('author', 'test commit', %s, %s)
            RETURNING id
            """,
            (action, user_id),
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


    """
    2. Commit table
    """

    def test_commit_table_exists(self, schema):
        """sensorthings.Commit table must be created by the auth script."""
        with schema.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'sensorthings' AND table_name = 'Commit'
                """
            )
            row = cur.fetchone()
        assert row is not None

    def test_commit_has_required_columns(self, schema):
        """Commit table must expose id, author, message, date, actionType, user_id."""
        with schema.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'sensorthings' AND table_name = 'Commit'
                """
            )
            cols = {r[0] for r in cur.fetchall()}
        for expected in ("id", "author", "message", "date", "actionType", "user_id"):
            assert expected in cols, f"Commit table missing column: {expected}"

    @pytest.mark.parametrize("action", ["CREATE", "UPDATE", "DELETE"])
    def test_commit_action_type_accepts_valid_values(self, schema, action):
        """All three valid actionType values must be accepted without error."""
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username=f"u-action-{action}")
            cid = self._insert_commit(cur, uid, action=action)
        assert isinstance(cid, int)

    def test_commit_action_type_rejects_invalid(self, schema):
        """actionType outside CREATE/UPDATE/DELETE must raise CheckViolation."""
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username="u-bad-action")
            with pytest.raises(psycopg2.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO sensorthings."Commit"
                        (author, message, "actionType", user_id)
                    VALUES ('a', 'msg', 'PATCH', %s)
                    """,
                    (uid,),
                )

    def test_commit_selflink_format(self, schema):
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username="sl-commit-user")
            cid = self._insert_commit(cur, uid)
            cur.execute(
                'SELECT "@iot.selfLink"(c) FROM sensorthings."Commit" c WHERE id = %s',
                (cid,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Commits({cid})"

    def test_commit_user_id_fk_enforced(self, schema):
        """Inserting a Commit with a non-existent user_id must raise ForeignKeyViolation."""
        with schema.cursor() as cur:
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                cur.execute(
                    """
                    INSERT INTO sensorthings."Commit"
                        (author, message, "actionType", user_id)
                    VALUES ('a', 'msg', 'CREATE', 999999)
                    """
                )

    def test_deleting_user_cascades_to_commit(self, schema):
        """Deleting a User must cascade-delete all their Commits."""
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username="u-cascade")
            cid = self._insert_commit(cur, uid)
            cur.execute('DELETE FROM sensorthings."User" WHERE id = %s', (uid,))
            cur.execute(
                'SELECT id FROM sensorthings."Commit" WHERE id = %s', (cid,)
            )
            row = cur.fetchone()
        assert row is None, "Commit must be deleted when its User is deleted"
