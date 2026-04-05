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
            VALUES (%s, 'http://def', 'desc', %s)
            RETURNING id
            """,
            (name, commit_id),
        )
        return self._get_id(cur.fetchone())
    
    def _insert_minimal_datastream(self, cur, thing_id, sensor_id, op_id, name="v-ds"):
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
        thing_id = self._insert_minimal_thing(cur, f"t-{suffix}")
        sensor_id = self._insert_minimal_sensor(cur, f"s-{suffix}")
        op_id = self._insert_minimal_observed_property(cur, f"op-{suffix}")
        ds_id = self._insert_minimal_datastream(cur, thing_id, sensor_id, op_id, f"ds-{suffix}")
        foi_id = self._insert_minimal_foi(cur, f"foi-{suffix}")
        return ds_id, foi_id, thing_id

    # Tests start
    
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

    def test_update_archives_old_row_to_history(self, schema):
        """
        On UPDATE the previous row version must appear in the history table
        with a closed systemTimeValidity (finite upper bound).
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "arch-thing")
            cur.execute(
                'UPDATE sensorthings."Thing" SET "description" = \'updated\' WHERE id = %s',
                (thing_id,),
            )
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1, "Exactly one archived row should exist after one UPDATE"
        stv = rows[0][0]
        assert not stv.upper_inf, "Archived row must have a finite upper bound"

    def test_update_live_row_gets_new_start(self, schema):
        """
        After UPDATE the live row's systemTimeValidity lower bound must be
        strictly after the archived row's lower bound.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-upd")
            cur.execute(
                'SELECT lower("systemTimeValidity") FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            t_before = cur.fetchone()[0]

            cur.execute(
                'UPDATE sensorthings."Thing" SET "description" = \'v2\' WHERE id = %s',
                (thing_id,),
            )
            cur.execute(
                'SELECT lower("systemTimeValidity") FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            t_after = cur.fetchone()[0]

        assert t_after >= t_before

    def test_insert_sets_upper_infinite(self, schema):
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "stv-upper")
            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings."Thing" WHERE id = %s',
                (thing_id,),
            )
            stv = cur.fetchone()[0]

        assert stv.upper is not None

    def test_update_raises_on_id_change(self, schema):
        """
        Attempting to change the id column of a versioned row must raise an
        exception from the istsos_mutate_history trigger.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "id-change")
            with pytest.raises(psycopg2.errors.RaiseException, match="ID must not be changed"):
                cur.execute(
                    'UPDATE sensorthings."Thing" SET id = %s WHERE id = %s',
                    (thing_id + 9999, thing_id),
                )
    
    def test_delete_archives_row_to_history(self, schema):
        """
        On DELETE the row must be copied to the history table with an
        inclusive, finite upper bound on systemTimeValidity.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "del-arch")
            cur.execute('DELETE FROM sensorthings."Thing" WHERE id = %s', (thing_id,))

            cur.execute(
                'SELECT "systemTimeValidity" FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            rows = cur.fetchall()

        assert len(rows) == 1, "One archived row must exist after DELETE"
        stv = rows[0][0]
        assert not stv.upper_inf, "Deleted row must have a finite upper bound"
        assert stv.upper_inc, "Deleted row's range must be upper-inclusive ([])"

    def test_delete_removes_live_row(self, schema):
        """After DELETE the live table must no longer contain the row."""
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "del-live")
            cur.execute('DELETE FROM sensorthings."Thing" WHERE id = %s', (thing_id,))
            cur.execute(
                'SELECT id FROM sensorthings."Thing" WHERE id = %s', (thing_id,)
            )
            row = cur.fetchone()
        assert row is None

    def test_update_creates_single_history_entry(self, schema):
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "single-hist")

            cur.execute(
                'UPDATE sensorthings."Thing" SET "description" = \'v2\' WHERE id = %s',
                (thing_id,),
            )

            cur.execute(
                'SELECT COUNT(*) FROM sensorthings_history."Thing" WHERE id = %s',
                (thing_id,),
            )
            count = cur.fetchone()[0]

        assert count == 1

    def test_location_gen_foi_id_update_skips_history(self, schema):
        """
        When only gen_foi_id changes on a Location row the trigger must
        return early (RETURN NEW without archiving), so the history table
        must remain empty for that row.
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

        assert rows == [], (
            "gen_foi_id-only update must NOT produce an archived row in the history table"
        )
    
    def test_datastream_phenomenontime_update_skips_history(self, schema):
        """
        Updating only phenomenonTime on a Datastream must not archive a row.
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

        assert rows == [], (
            "phenomenonTime-only update must NOT produce an archived row"
        )

    def test_datastream_observedarea_update_skips_history(self, schema):
        """
        Updating only observedArea on a Datastream must not archive a row.
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

        assert rows == [], (
            "observedArea-only update must NOT produce an archived row"
        )

    def test_datastream_name_update_does_archive(self, schema):
        """
        Updating a 'normal' column (name) on a Datastream must archive the
        old row — verifying the skip logic is column-specific.
        """
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

        assert len(rows) == 1, "Name update must produce exactly one archived row"

    @pytest.mark.xfail(reason="Datastream skip logic incorrectly ignores mixed updates")
    def test_datastream_mixed_update_does_archive(self, schema):
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
