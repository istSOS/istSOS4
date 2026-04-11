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

# STA tables that auth adds a commit_id column to
COMMIT_ID_TABLES = [
    "Location",
    "Thing",
    "HistoricalLocation",
    "ObservedProperty",
    "Sensor",
    "Datastream",
    "FeaturesOfInterest",
    "Observation",
]

# Tables that must have RLS enabled
RLS_TABLES = [
    "Location",
    "Thing",
    "HistoricalLocation",
    "ObservedProperty",
    "Sensor",
    "Datastream",
    "FeaturesOfInterest",
    "Observation",
]

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
        cur.execute("SELECT set_config('custom.authorization', 'true', false)")
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
    
    def _insert_minimal_location(self, cur, commit_id, name="test-loc"):
        cur.execute(
            """
            INSERT INTO sensorthings."Location"
                (name, description, "encodingType", location, commit_id)
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326), %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())
    
    def _insert_minimal_thing(self, cur, commit_id, name="test-thing"):
        cur.execute(
            """
            INSERT INTO sensorthings."Thing"
                (name, description, commit_id)
            VALUES (%s, 'desc', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_sensor(self, cur, commit_id, name="test-sensor"):
        cur.execute(
            """
            INSERT INTO sensorthings."Sensor"
                (name, description, "encodingType", metadata, commit_id)
            VALUES (%s, 'desc', 'application/pdf', 'http://meta', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_observed_property(self, cur, commit_id, name="test-op"):
        cur.execute(
            """
            INSERT INTO sensorthings."ObservedProperty"
                (name, definition, description, commit_id)
            VALUES (%s, 'http://def', 'desc', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_datastream(self, cur, thing_id, sensor_id, op_id,
                                   commit_id=None, name="test-ds"):
        cur.execute(
            """
            INSERT INTO sensorthings."Datastream"
                (name, description, "unitOfMeasurement",
                 "observationType", thing_id, sensor_id,
                 "observedproperty_id", commit_id)
            VALUES (%s, 'desc',
                    '{"name":"C","symbol":"C","definition":"http://d"}'::jsonb,
                    'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                    %s, %s, %s, %s)
            RETURNING id
            """,
            (name, thing_id, sensor_id, op_id, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_foi(self, cur, commit_id=None, name="test-foi"):
        cur.execute(
            """
            INSERT INTO sensorthings."FeaturesOfInterest"
                (name, description, "encodingType", feature, commit_id)
            VALUES (%s, 'desc', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(9.0, 46.0), 4326), %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _setup_entities(self, cur, suffix="auth"):
        """Insert a User + Commit + full STA entity chain. Returns (uid, cid, thing_id, sensor_id, op_id, ds_id, foi_id)."""
        uid = self._insert_user(cur, username=f"u-{suffix}")
        cid = self._insert_commit(cur, uid)
        thing_id = self._insert_minimal_thing(cur, cid, name=f"t-{suffix}")
        sensor_id = self._insert_minimal_sensor(cur, cid, name=f"s-{suffix}")
        op_id = self._insert_minimal_observed_property(cur, cid, name=f"op-{suffix}")
        ds_id = self._insert_minimal_datastream(
            cur, thing_id, sensor_id, op_id, commit_id=cid, name=f"ds-{suffix}"
        )
        foi_id = self._insert_minimal_foi(cur, commit_id=cid, name=f"foi-{suffix}")
        return uid, cid, thing_id, sensor_id, op_id, ds_id, foi_id
    
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
        """@iot.selfLink for Commit must return '/Commits(<id>)'."""
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


    """
    3. commit_id FK columns on all STA entity tables
    """

    def test_commit_id_column_present_on_all_sta_tables(self, schema):
        """commit_id column must exist on every STA entity table."""
        with schema.cursor() as cur:
            for table in COMMIT_ID_TABLES:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'sensorthings'
                      AND table_name = %s AND column_name = 'commit_id'
                    """,
                    (table,),
                )
                assert cur.fetchone() is not None, (
                    f"commit_id column missing on {table}"
                )

    @pytest.mark.parametrize(
        "table, expected_nullable",
        [
            ("Thing", "NO"),
            ("Location", "NO"),
            ("Sensor", "NO"),
            ("ObservedProperty", "NO"),
            ("Datastream", "YES"),
            ("Observation", "YES"),
            ("FeaturesOfInterest", "YES"),
        ],
    )
    def test_commit_id_nullability(self, schema, table, expected_nullable):
        with schema.cursor() as cur:
            cur.execute(
                """
                SELECT is_nullable FROM information_schema.columns
                WHERE table_schema = 'sensorthings'
                AND table_name = %s AND column_name = 'commit_id'
                """,
                (table,),
            )
            assert cur.fetchone()[0] == expected_nullable

    def test_commit_id_fk_rejects_orphan_on_thing(self, schema):
        """Inserting a Thing with a non-existent commit_id must raise ForeignKeyViolation."""
        with schema.cursor() as cur:
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                cur.execute(
                    """
                    INSERT INTO sensorthings."Thing"
                        (name, description, commit_id)
                    VALUES ('orphan', 'desc', 999999)
                    """
                )

    """
    4. Commit@iot.navigationLink on each STA entity
    """

    @pytest.mark.parametrize(
        "table, alias, insert_fn, path",
        [
            ("Location", "l", "_insert_minimal_location", "Locations"),
            ("Thing", "t", "_insert_minimal_thing", "Things"),
            ("Sensor", "s", "_insert_minimal_sensor", "Sensors"),
            ("ObservedProperty", "op", "_insert_minimal_observed_property", "ObservedProperties"),
            ("FeaturesOfInterest", "f", "_insert_minimal_foi", "FeaturesOfInterest"),
        ],
    )
    def test_commit_nav_link(self, schema, table, alias, insert_fn, path):
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username=f"u-{table}-nav")
            cid = self._insert_commit(cur, uid)

            fn = getattr(self, insert_fn)

            # FOI needs commit_id explicitly
            if table == "FeaturesOfInterest":
                entity_id = fn(cur, commit_id=cid, name=f"nav-{table}")
            else:
                entity_id = fn(cur, cid, name=f"nav-{table}")

            cur.execute(
                f'SELECT "Commit@iot.navigationLink"({alias}) '
                f'FROM sensorthings."{table}" {alias} WHERE id = %s',
                (entity_id,),
            )
            link = cur.fetchone()[0]

        assert link == f"/{path}({entity_id})/Commit({cid})"
    
    def test_datastream_commit_nav_link(self, schema):
        with schema.cursor() as cur:
            _, cid, _, _, _, ds_id, _ = self._setup_entities(cur, suffix="ds-nav")

            cur.execute(
                'SELECT "Commit@iot.navigationLink"(ds) '
                'FROM sensorthings."Datastream" ds WHERE id = %s',
                (ds_id,),
            )
            link = cur.fetchone()[0]

        assert link == f"/Datastreams({ds_id})/Commit({cid})"
    
    def test_datastream_commit_nav_link_null_when_commit_id_null(self, schema):
        with schema.cursor() as cur:
            _, _, tid, sid, op_id, _, _ = self._setup_entities(cur, suffix="ds-null-nav")

            ds_id = self._insert_minimal_datastream(
                cur, tid, sid, op_id, commit_id=None, name="ds-no-commit"
            )

            cur.execute(
                'SELECT "Commit@iot.navigationLink"(ds) '
                'FROM sensorthings."Datastream" ds WHERE id = %s',
                (ds_id,),
            )
            link = cur.fetchone()[0]

        assert link is None
    
    """
    5. Reverse nav functions on Commit
    """

    @pytest.mark.parametrize(
        "entity, insert_fn, nav_fn, expected_path, should_exist",
        [
            ("Thing", "_insert_minimal_thing", "Things@iot.navigationLink", "Things", True),
            ("Thing", None, "Things@iot.navigationLink", "Things", False),

            ("Location", "_insert_minimal_location", "Locations@iot.navigationLink", "Locations", True),

            ("Sensor", "_insert_minimal_sensor", "Sensors@iot.navigationLink", "Sensors", True),

            ("ObservedProperty", "_insert_minimal_observed_property",
            "ObservedProperties@iot.navigationLink", "ObservedProperties", True),

            ("Datastream", None, "Datastreams@iot.navigationLink", "Datastreams", False),

            ("Observation", None, "Observations@iot.navigationLink", "Observations", False),
        ],
    )
    def test_commit_reverse_navigation_links(
        self, schema, entity, insert_fn, nav_fn, expected_path, should_exist
    ):
        """
        Reverse navigation links from Commit should:
        - Return correct path when entity references the commit
        - Return NULL when no entity references the commit
        """
        with schema.cursor() as cur:
            uid = self._insert_user(cur, username=f"u-rev-{entity}")
            cid = self._insert_commit(cur, uid)

            # insert_fn: helper to insert entity linked to this commit (cur, commit_id, name)
            if insert_fn:
                fn = getattr(self, insert_fn)
                fn(cur, cid, name=f"rev-{entity.lower()}")

            cur.execute(
                f'SELECT "{nav_fn}"(c) FROM sensorthings."Commit" c WHERE id = %s',
                (cid,),
            )
            link = cur.fetchone()[0]

        if should_exist:
            assert link == f"/Commits({cid})/{expected_path}"
        else:
            assert link is None

    """
    6. Role existence and privilege grants
    """

    @pytest.mark.parametrize("role", ["user", "guest", "sensor"])
    def test_roles_exist(self, schema, role):
        """All required roles must be created by the auth script."""
        with schema.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            assert cur.fetchone() is not None
    
    @pytest.mark.parametrize(
        "role, table, privilege, expected",
        [
            ("guest",  'sensorthings."User"',        "SELECT", False),
            ("guest",  'sensorthings."Thing"',       "SELECT", True),

            ("user",   'sensorthings."User"',        "INSERT", False),
            ("user",   'sensorthings."Commit"',      "UPDATE", False),

            ("sensor", 'sensorthings."User"',        "SELECT", False),
            ("sensor", 'sensorthings."Observation"', "INSERT", True),
            ("sensor", 'sensorthings."Commit"',      "INSERT", True),
        ],
    )
    def test_role_table_privileges(self, schema, role, table, privilege, expected):
        """Validate role-based privileges across tables."""
        with schema.cursor() as cur:
            cur.execute(
                "SELECT has_table_privilege(%s, %s, %s)",
                (role, table, privilege),
            )
            assert cur.fetchone()[0] is expected

    """
    7. Row-level security
    """

    def test_rls_enabled_on_all_core_tables(self, schema):
        """RLS must be enabled on every core STA table after auth loads."""
        with schema.cursor() as cur:
            for table in RLS_TABLES:
                cur.execute(
                    """
                    SELECT relrowsecurity
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'sensorthings' AND c.relname = %s
                    """,
                    (table,),
                )
                row = cur.fetchone()
                assert row is not None and row[0] is True, (
                    f"RLS not enabled on {table}"
                )

    def test_anonymous_guest_policy_exists_for_all_core_tables(self, schema):
        """
        anonymous_<table> SELECT policy for 'guest' must exist on every core table.

        NOTE (IMPORTANT):
        PostgreSQL lowercases the identifiers when stored in pg_policies. So even if
        policies are created as 'anonymous_Location' in the istsos_auth.sql file,
        they appear as 'anonymous_location' in pg_policies and the tests take that into account.
        """
        with schema.cursor() as cur:
            for table in RLS_TABLES:
                cur.execute(
                    """
                    SELECT 1 FROM pg_policies
                    WHERE schemaname = 'sensorthings'
                    AND tablename = %s
                    AND policyname = %s
                    """,
                    (table, f"anonymous_{table.lower()}"),
                )
                assert cur.fetchone() is not None, (
                    f"anonymous_{table.lower()} policy missing"
                )

    """
    8. Policy generation functions
    """
    
    @pytest.mark.xfail(
        reason="Policy functions rely on unsafe current_setting('custom.network') access",
        strict=True,
    )
    @pytest.mark.parametrize(
        "fn_name, pname, expected_count",
        [
            ("viewer_policy", "test-viewer", len(RLS_TABLES)),
            ("editor_policy", "test-editor", len(RLS_TABLES)),
            ("sensor_policy", "test-sensor-pol", 12),
            ("obs_manager_policy", "test-obsmgr-pol", 11),
        ],
    )
    def test_policy_creates_expected_number_of_policies(self, schema, fn_name, pname, expected_count):
        """
        ============================================================
        POLICY GENERATION CONTRACT TEST

        This test validates that each policy helper function creates
        the correct number of policies in pg_policies.

        Breakdown:
        - viewer_policy:
            → 1 SELECT policy per RLS table

        - editor_policy:
            → 1 ALL policy per RLS table

        - sensor_policy:
            → SELECT on all RLS tables
            → + INSERT (Observation, FeaturesOfInterest)
            → + UPDATE (Datastream, Location)
            → = 12 total

        - obs_manager_policy:
            → SELECT on all non-Observation tables
            → + ALL on Observation
            → + INSERT FOI
            → + UPDATE (Datastream, Location)
            → = 11 total

        NOTE:
        We validate via policy count using a prefix (policyname LIKE pname%).
        ============================================================
        """
        with schema.cursor() as cur:
            cur.execute(
                f"SELECT sensorthings.{fn_name}(%s::text[], %s)",
                (["guest"], pname),
            )
            cur.execute(
                """
                SELECT count(*) FROM pg_policies
                WHERE schemaname = 'sensorthings'
                AND policyname LIKE %s
                """,
                (f"{pname}%",),
            )
            count = cur.fetchone()[0]

        assert count == expected_count

    """
    9. remove_user_from_policy and add_users_to_policy
    """
    
    @pytest.mark.xfail(
        reason="viewer_policy uses unsafe current_setting('custom.network') without fallback",
        strict=True,
    )
    def test_remove_user_from_policy_drops_policy_when_sole_role(self, schema):
        """
        remove_user_from_policy must DROP a policy entirely when the removed
        role was the only member, leaving no dangling empty policy.
        """
        pname = "test-remove-pol"
        with schema.cursor() as cur:
            cur.execute(
                "SELECT sensorthings.viewer_policy(%s::text[], %s)",
                (["guest"], pname),
            )
            cur.execute(
                "SELECT sensorthings.remove_user_from_policy('guest')"
            )
            cur.execute(
                """
                SELECT count(*) FROM pg_policies
                WHERE schemaname = 'sensorthings'
                  AND policyname LIKE %s
                """,
                (f"{pname}%",),
            )
            count = cur.fetchone()[0]
        assert count == 0, "All policies must be dropped when the last role is removed"

    def test_add_users_to_policy_raises_for_missing_policy(self, schema):
        """add_users_to_policy must raise an error (ERRCODE 42704) for a non-existent policy name."""
        with schema.cursor() as cur:
            with pytest.raises(psycopg2.errors.UndefinedObject):
                cur.execute(
                    "SELECT sensorthings.add_users_to_policy(%s::text[], %s)",
                    (["guest"], "nonexistent_policy_xyz"),
                )

    """
    10. Btree indexes on commit_id
    """

    def test_commit_id_indexes_exist_for_all_sta_tables(self, schema):
        """Btree index on commit_id must exist for every STA entity table."""
        expected = {
            "Location": "idx_location_commit_id",
            "Thing": "idx_thing_commit_id",
            "HistoricalLocation": "idx_historicallocation_commit_id",
            "ObservedProperty": "idx_observedproperty_commit_id",
            "Sensor": "idx_sensor_commit_id",
            "Datastream": "idx_datastream_commit_id",
            "FeaturesOfInterest": "idx_featuresofinterest_commit_id",
            "Observation": "idx_observation_commit_id",
        }
        with schema.cursor() as cur:
            for table, idx in expected.items():
                cur.execute(
                    """
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = 'sensorthings' AND indexname = %s
                    """,
                    (idx,),
                )
                assert cur.fetchone() is not None, (
                    f"Index {idx} missing for {table}"
                )
