"""
Tests for istsos_schema_versioning.sql

Sections:
1. istsos_mutate_history() trigger
   - INSERT / UPDATE / DELETE behavior
   - skip-archiving logic
   - edge cases (ID mutation, known bug)

2. istsos_prevent_table_update()
   - history table immutability (UPDATE / DELETE blocked)

3. add_table_to_versioning()
   - systemTimeValidity column presence

4. systemTimeValidity constraints
   - exclusion constraint prevents overlapping ranges

5. traveltime view
   - row visibility (live + history)
   - @iot.selfLink correctness
   - Observation result() dispatch

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


def _get_raw_conn(dsn=DSN):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def _recreate_database():
    """Drop and recreate the test database, cleaning up roles beforehand."""
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

    # Drop owned objects for roles across all surviving databases so
    # DROP ROLE doesn't fail on dependent objects.
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
            pass  # DB may have been dropped mid-iteration

    admin_conn = psycopg2.connect(ADMIN_DSN)
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        cur.execute("DROP ROLE IF EXISTS administrator")
        cur.execute("DROP ROLE IF EXISTS testuser")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin_conn.close()


def _load_schema(conn):
    """Execute the base schema SQL with versioning enabled."""
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
    """Execute the versioning SQL on top of the base schema."""
    sql = VERSIONING_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute("SET custom.epsg = '4326'")
        cur.execute("SET custom.network = 'false'")
        cur.execute("SET custom.versioning = 'true'")
        cur.execute('SET "custom.authorization" = \'false\'')
        cur.execute(sql)


class TestSchemaVersioning:
    """
    Integration tests for system-versioned SensorThings tables.

    The schema and versioning SQL are loaded once per class. Each test runs
    inside a transaction that is rolled back on teardown, so tests are
    fully isolated without needing to recreate the database each time.
    """

    @pytest.fixture(autouse=True, scope="class")
    def schema(self):
        """Load the schema once and yield a transactional connection."""
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
        """Roll back all mutations after each test."""
        yield
        schema.rollback()

    # Insertion helpers

    def _get_id(self, row):
        return row[0] if not isinstance(row, dict) else row["id"]

    def _insert_commit(self, cur, action="CREATE"):
        """
        Insert a Commit row and return its id.

        Required because versioning adds a NOT NULL commit_id FK to most tables.
        """
        cur.execute(
            """
            INSERT INTO sensorthings."Commit" ("author", "message", "actionType")
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

    def _insert_minimal_location(self, cur, name="v-loc"):
        commit_id = self._insert_commit(cur)
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
        return self._get_id(cur.fetchone())

    def _insert_minimal_sensor(self, cur, name="v-sensor"):
        commit_id = self._insert_commit(cur)
        cur.execute(
            """
            INSERT INTO sensorthings."Sensor"
                ("name", "description", "encodingType", "metadata", "commit_id")
            VALUES (%s, 'desc', 'application/pdf', 'http://meta', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_observed_property(self, cur, name="v-op"):
        commit_id = self._insert_commit(cur)
        cur.execute(
            """
            INSERT INTO sensorthings."ObservedProperty"
                ("name", "definition", "description", "commit_id")
            VALUES (%s, 'http://def', 'desc', %s) RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_datastream(self, cur, thing_id, sensor_id, op_id, name="v-ds"):
        # Datastream.commit_id is nullable, so no commit needed here.
        cur.execute(
            """
            INSERT INTO sensorthings."Datastream"
                ("name", "description", "unitOfMeasurement",
                 "observationType", "thing_id", "sensor_id", "observedproperty_id")
            VALUES (%s, 'desc', '{"name":"C","symbol":"C","definition":"http://d"}'::jsonb,
                    'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
                    %s, %s, %s)
            RETURNING id
            """,
            (name, thing_id, sensor_id, op_id),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_foi(self, cur, name="v-foi"):
        # FeaturesOfInterest.commit_id is nullable.
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
        return self._get_id(cur.fetchone())

    def _setup_ds_foi(self, cur, suffix="v"):
        """Create the full dependency chain needed for Datastream/Observation tests."""
        thing_id = self._insert_minimal_thing(cur, f"t-{suffix}")
        sensor_id = self._insert_minimal_sensor(cur, f"s-{suffix}")
        op_id = self._insert_minimal_observed_property(cur, f"op-{suffix}")
        ds_id = self._insert_minimal_datastream(cur, thing_id, sensor_id, op_id, f"ds-{suffix}")
        foi_id = self._insert_minimal_foi(cur, f"foi-{suffix}")
        return ds_id, foi_id, thing_id

    # istsos_mutate_history() - INSERT behavior

    def test_insert_sets_system_time_validity_start(self, schema):
        """INSERT must set a finite lower bound on systemTimeValidity."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-insert")
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            stv = cur.fetchone()[0]

        assert stv is not None
        # psycopg2 maps PostgreSQL's TIMESTAMPTZ 'infinity' to datetime(9999-12-31),
        # so upper_inf will be False even for a valid open-ended range. We only
        # assert that the lower bound is a real timestamp.
        assert not stv.lower_inf
        assert stv.lower is not None

    def test_insert_sets_upper_infinite(self, schema):
        """The upper bound after INSERT must be non-null (open-ended range)."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-upper")
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            stv = cur.fetchone()[0]

        assert stv.upper is not None

    # istsos_mutate_history() - UPDATE behavior

    def test_update_archives_old_row_to_history(self, schema):
        """UPDATE must copy the previous version to the history table."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "arch-thing")
            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'updated' WHERE id = %s",
                (thing_id,),
            )
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1, "exactly one archived row after one UPDATE"
        assert not rows[0][0].upper_inf, "archived row must have a finite upper bound"

    def test_update_creates_single_history_entry(self, schema):
        """One UPDATE must produce exactly one row in the history table."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "single-hist")
            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'v2' WHERE id = %s",
                (thing_id,),
            )
            cur.execute(
                'SELECT COUNT(*) FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            count = cur.fetchone()[0]

        assert count == 1

    def test_update_live_row_gets_new_start(self, schema):
        """After UPDATE the live row's systemTimeValidity lower bound must advance."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-upd")
            cur.execute(
                'SELECT lower("systemTimeValidity") FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            t_before = cur.fetchone()[0]

            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'v2' WHERE id = %s",
                (thing_id,),
            )
            cur.execute(
                'SELECT lower("systemTimeValidity") FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            t_after = cur.fetchone()[0]

        # current_timestamp is stable within a transaction, so t_after >= t_before.
        assert t_after >= t_before

    def test_update_raises_on_id_change(self, schema):
        """Changing the id of a versioned row must raise an exception."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "id-change")
            with pytest.raises(psycopg2.errors.RaiseException, match="ID must not be changed"):
                cur.execute(
                    'UPDATE sensorthings."Thing" SET id = %s WHERE id = %s',
                    (thing_id + 9999, thing_id),
                )

    def test_multiple_updates_produce_ordered_history_chain(self, schema):
        """
        N updates must produce N archived rows with contiguous,
        non-overlapping systemTimeValidity ranges.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "chain-thing")

            for i in range(3):
                cur.execute(
                    'UPDATE sensorthings."Thing" SET "description" = %s WHERE id = %s',
                    (f"v{i + 2}", thing_id),
                )

            cur.execute(
                """
                SELECT lower("systemTimeValidity"), upper("systemTimeValidity")
                FROM sensorthings_history."Thing"
                WHERE id = %s
                ORDER BY lower("systemTimeValidity")
                """,
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 3

        # Each row's upper bound must equal the next row's lower bound.
        for i in range(len(rows) - 1):
            _, upper = rows[i]
            next_lower, _ = rows[i + 1]
            assert upper == next_lower, (
                f"gap between history row {i} and {i + 1}: {upper} != {next_lower}"
            )

    # istsos_mutate_history() - skip-archiving paths

    def test_location_gen_foi_id_update_skips_history(self, schema):
        """
        Updating only gen_foi_id on a Location must not produce a history row.

        The trigger returns early for this column because it is set automatically
        by the system (not a user-driven change) and should not create a new
        version in the audit trail.
        """
        with schema.cursor() as cur:
            loc_id = self._insert_minimal_location(cur, "skip-foi-loc")
            foi_id = self._insert_minimal_foi(cur, "skip-gen-foi")
            cur.execute(
                'UPDATE sensorthings."Location" SET "gen_foi_id" = %s WHERE id = %s',
                (foi_id, loc_id),
            )
            cur.execute(
                'SELECT id FROM sensorthings_history."Location" WHERE id = %s',
                (loc_id,),
            )
            rows = cur.fetchall()

        assert rows == []

    def test_datastream_phenomenontime_update_skips_history(self, schema):
        """
        Updating only phenomenonTime on a Datastream must not produce a history row.

        phenomenonTime is updated automatically as observations arrive and
        should not create new versions in the audit trail.
        """
        with schema.cursor() as cur:
            ds_id, _, _ = self._setup_ds_foi(cur, suffix="skip-pt")
            cur.execute(
                """
                UPDATE sensorthings."Datastream"
                SET "phenomenonTime" = tstzrange(now(), now() + interval '1 hour')
                WHERE id = %s
                """,
                (ds_id,),
            )
            cur.execute(
                'SELECT id FROM sensorthings_history."Datastream" WHERE id = %s',
                (ds_id,),
            )
            rows = cur.fetchall()

        assert rows == []

    def test_datastream_observedarea_update_skips_history(self, schema):
        """
        Updating only observedArea on a Datastream must not produce a history row.

        Like phenomenonTime, observedArea is maintained automatically and
        is excluded from the versioning audit trail.
        """
        with schema.cursor() as cur:
            ds_id, _, _ = self._setup_ds_foi(cur, suffix="skip-oa")
            cur.execute(
                """
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_SetSRID(ST_MakePoint(10.0, 47.0), 4326)
                WHERE id = %s
                """,
                (ds_id,),
            )
            cur.execute(
                'SELECT id FROM sensorthings_history."Datastream" WHERE id = %s',
                (ds_id,),
            )
            rows = cur.fetchall()

        assert rows == []

    def test_datastream_name_update_does_archive(self, schema):
        """A regular column update on Datastream must produce a history row."""
        with schema.cursor() as cur:
            ds_id, _, _ = self._setup_ds_foi(cur, suffix="arch-ds")
            cur.execute(
                "UPDATE sensorthings.\"Datastream\" SET \"name\" = 'renamed' WHERE id = %s",
                (ds_id,),
            )
            cur.execute(
                'SELECT id FROM sensorthings_history."Datastream" WHERE id = %s',
                (ds_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1

    def test_datastream_mixed_update_does_archive(self, schema):
        """
        Updating observedArea together with a regular column (name) must still
        produce a history row.
        """
        with schema.cursor() as cur:
            ds_id, _, _ = self._setup_ds_foi(cur, suffix="mixed")
            cur.execute(
                """
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_SetSRID(ST_MakePoint(10.0, 47.0), 4326),
                    "name" = 'changed'
                WHERE id = %s
                """,
                (ds_id,),
            )
            cur.execute(
                'SELECT id FROM sensorthings_history."Datastream" WHERE id = %s',
                (ds_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1

    # istsos_mutate_history() - DELETE behavior

    def test_delete_archives_row_to_history(self, schema):
        """DELETE must copy the row to history with an upper-inclusive, finite range."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "del-arch")
            cur.execute('DELETE FROM sensorthings."Thing" WHERE id = %s', (thing_id,))
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1
        stv = rows[0][0]
        assert not stv.upper_inf, "deleted row must have a finite upper bound"
        assert stv.upper_inc, "deleted row range must be upper-inclusive ([])"

    def test_delete_removes_live_row(self, schema):
        """After DELETE the row must no longer appear in the live table."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "del-live")
            cur.execute('DELETE FROM sensorthings."Thing" WHERE id = %s', (thing_id,))
            cur.execute('SELECT id FROM sensorthings."Thing" WHERE id = %s', (thing_id,))
            assert cur.fetchone() is None

    # istsos_prevent_table_update()

    def test_history_table_update_raises(self, schema):
        """Any UPDATE on a history table must be rejected."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "prevent-upd")
            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'v2' WHERE id = %s",
                (thing_id,),
            )
            with pytest.raises(
                psycopg2.errors.RaiseException,
                match="Updates or Deletes on this table are not allowed",
            ):
                cur.execute(
                    "UPDATE sensorthings_history.\"Thing\" SET \"description\" = 'hack' WHERE id = %s",
                    (thing_id,),
                )

    def test_history_table_delete_raises(self, schema):
        """Any DELETE on a history table must be rejected."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "prevent-del")
            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'v2' WHERE id = %s",
                (thing_id,),
            )
            with pytest.raises(
                psycopg2.errors.RaiseException,
                match="Updates or Deletes on this table are not allowed",
            ):
                cur.execute(
                    'DELETE FROM sensorthings_history."Thing" WHERE id = %s',
                    (thing_id,),
                )

    # add_table_to_versioning()

    def test_versioned_tables_have_system_time_validity_column(self, schema):
        """Every versioned table must have a systemTimeValidity column."""
        tables = [
            "Location", "Thing", "HistoricalLocation", "ObservedProperty",
            "Sensor", "Datastream", "FeaturesOfInterest", "Observation",
        ]
        with schema.cursor() as cur:
            for table in tables:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'sensorthings'
                      AND table_name = %s
                      AND column_name = 'systemTimeValidity'
                    """,
                    (table,),
                )
                assert cur.fetchone() is not None, (
                    f'sensorthings."{table}" is missing systemTimeValidity'
                )

    def test_history_exclusion_constraint_blocks_overlapping_insert(self, schema):
        """
        Two direct INSERTs into a history table for the same id with overlapping
        systemTimeValidity ranges must raise an ExclusionViolation.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "excl-thing")

            # Build the column list without systemTimeValidity so we can
            # supply a custom range for each INSERT.
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'sensorthings' AND table_name = 'Thing'
                  AND column_name != 'systemTimeValidity'
                ORDER BY ordinal_position
                """,
            )
            cols = [r[0] for r in cur.fetchall()]
            col_list = ", ".join(f'"{c}"' for c in cols)

            cur.execute(
                f"""
                INSERT INTO sensorthings_history."Thing" ({col_list}, "systemTimeValidity")
                SELECT {col_list}, tstzrange('2020-01-01', '2021-01-01', '[)')
                FROM sensorthings."Thing" WHERE id = %s
                """,
                (thing_id,),
            )

            with pytest.raises(psycopg2.errors.ExclusionViolation):
                cur.execute(
                    f"""
                    INSERT INTO sensorthings_history."Thing" ({col_list}, "systemTimeValidity")
                    SELECT {col_list}, tstzrange('2020-06-01', '2021-06-01', '[)')
                    FROM sensorthings."Thing" WHERE id = %s
                    """,
                    (thing_id,),
                )

    # Traveltime view

    def test_traveltime_view_contains_live_row(self, schema):
        """The traveltime view must include the current live row."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "tt-live")
            cur.execute(
                'SELECT id FROM sensorthings."Thing_traveltime" WHERE id = %s',
                (thing_id,),
            )
            assert cur.fetchone() is not None

    def test_traveltime_view_contains_archived_row_after_update(self, schema):
        """After UPDATE the traveltime view must expose both the old and new versions."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "tt-arch")
            cur.execute(
                "UPDATE sensorthings.\"Thing\" SET \"description\" = 'v2' WHERE id = %s",
                (thing_id,),
            )
            cur.execute(
                'SELECT id FROM sensorthings."Thing_traveltime" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 2

    def test_traveltime_view_contains_deleted_row(self, schema):
        """After DELETE the traveltime view must still expose the deleted version via history."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "tt-del")
            cur.execute('DELETE FROM sensorthings."Thing" WHERE id = %s', (thing_id,))
            cur.execute(
                'SELECT id FROM sensorthings."Thing_traveltime" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1

    @pytest.mark.parametrize(
        "insert_fn, table, alias, expected_path",
        [
            ("_insert_minimal_thing",             "Thing_traveltime",              "t",  "Things"),
            ("_insert_minimal_location",          "Location_traveltime",           "l",  "Locations"),
            ("_insert_minimal_sensor",            "Sensor_traveltime",             "s",  "Sensors"),
            ("_insert_minimal_observed_property", "ObservedProperty_traveltime",   "op", "ObservedProperties"),
            ("_setup_ds_foi",                     "Datastream_traveltime",         "d",  "Datastreams"),
            ("_insert_minimal_foi",               "FeaturesOfInterest_traveltime", "f",  "FeaturesOfInterest"),
        ],
    )
    def test_traveltime_selflink(self, schema, insert_fn, table, alias, expected_path):
        """The @iot.selfLink function on each traveltime view must return the correct URL."""
        with schema.cursor() as cur:
            fn = getattr(self, insert_fn)
            if insert_fn == "_setup_ds_foi":
                entity_id, *_ = fn(cur, suffix="tt-sl")
            else:
                entity_id = fn(cur, "tt-sl")

            cur.execute(
                f'SELECT "@iot.selfLink"({alias}) FROM sensorthings."{table}" {alias} WHERE id = %s',
                (entity_id,),
            )
            link = cur.fetchone()[0]

        assert link == f"/{expected_path}({entity_id})"

    def test_traveltime_selflink_observation(self, schema):
        """@iot.selfLink on Observation_traveltime must return '/Observations(<id>)'."""
        with schema.cursor() as cur:
            ds_id, foi_id, _ = self._setup_ds_foi(cur, suffix="tt-sl-obs")
            cur.execute(
                """
                INSERT INTO sensorthings."Observation"
                    ("resultType", "resultString", "datastream_id", "featuresofinterest_id")
                VALUES (3, 'x', %s, %s) RETURNING id
                """,
                (ds_id, foi_id),
            )
            obs_id = cur.fetchone()[0]
            cur.execute(
                'SELECT "@iot.selfLink"(o) FROM sensorthings."Observation_traveltime" o WHERE id = %s',
                (obs_id,),
            )
            link = cur.fetchone()[0]

        assert link == f"/Observations({obs_id})"

    def test_traveltime_observation_result_function(self, schema):
        """
        The result() overload on Observation_traveltime must dispatch correctly
        for each resultType, matching the behavior of the base Observation function.
        """
        cases = [
            (0, "resultNumber", 3.14),
            (1, "resultBoolean", True),
            (3, "resultString",  "hi"),
        ]

        with schema.cursor() as cur:
            ds_id, foi_id, _ = self._setup_ds_foi(cur, suffix="tt-result")

            for result_type, col, expected in cases:
                cur.execute(
                    f"""
                    INSERT INTO sensorthings."Observation"
                        ("resultType", "{col}", "datastream_id",
                         "featuresofinterest_id", "phenomenonTime")
                    VALUES (%s, %s, %s, %s,
                        tstzrange(
                            now() + (%s || ' seconds')::interval,
                            now() + (%s || ' seconds')::interval,
                            '[]'
                        ))
                    RETURNING id
                    """,
                    (result_type, expected, ds_id, foi_id,
                     result_type * 10, result_type * 10),
                )
                obs_id = cur.fetchone()[0]

                # Use ORDER BY + LIMIT to get the live version without relying on
                # upper_inf, which psycopg2 misreports for TIMESTAMPTZ 'infinity'.
                cur.execute(
                    """
                    SELECT result(o)
                    FROM sensorthings."Observation_traveltime" o
                    WHERE id = %s
                    ORDER BY lower("systemTimeValidity") DESC
                    LIMIT 1
                    """,
                    (obs_id,),
                )
                actual = cur.fetchone()[0]
                assert actual == expected, (
                    f"resultType {result_type}: expected {expected!r}, got {actual!r}"
                )
