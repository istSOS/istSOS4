"""
test(schema): direct psycopg2 tests for istsos_schema.sql logic

Tests:
  1. result() dispatch on Observation for all resultType variants (0-3 + invalid)
  2. delete_related_historical_locations trigger cascade
  3. @iot.selfLink computed functions for all main entities
  4. expand() nextLink pagination sentinel (present vs absent)

Run with:
  TEST_DB_DSN=postgresql://... pytest test_schema.py -v
  or rely on the default: postgresql://postgres:postgres@localhost:5432/istsos_test
"""

import json
import os
import pathlib
import psycopg2
import psycopg2.extras
import pytest

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "database" / "istsos_schema.sql"


DSN = "postgresql://postgres:15889@localhost:5432/istsos_test"
ADMIN_DSN = "postgresql://postgres:15889@localhost:5432/postgres"
TEST_DB = "istsos_test"


def _get_raw_conn():
    """Open a connection with autocommit so we can run DDL freely."""
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

        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")

        # roles are cluster-level and survive DROP DATABASE
        # drop them here so the schema's CREATE ROLE always succeeds
        cur.execute("DROP ROLE IF EXISTS administrator")
        cur.execute("DROP ROLE IF EXISTS testuser")

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

class TestSchema:
    """
    All tests share a single schema load per pytest session.
    Each test method rolls back its own data changes.
    """

    @pytest.fixture(autouse=True, scope="function")
    def schema(self):
        _recreate_database()

        # 🔥 connection 1: setup
        setup_conn = _get_raw_conn()
        _load_schema(setup_conn)
        setup_conn.close()

        # 🔥 connection 2: test usage
        conn = psycopg2.connect(DSN)
        conn.autocommit = False

        yield conn

        conn.close()
    
    def _insert_minimal_location(self, cur, name="test-loc"):
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
        return self._get_id(cur.fetchone())

    def _insert_minimal_thing(self, cur, name="test-thing"):
        cur.execute(
            """
            INSERT INTO sensorthings."Thing" ("name", "description")
            VALUES (%s, 'desc')
            RETURNING id
            """,
            (name,),
        )
        return self._get_id(cur.fetchone())
    
    def _get_id(self, row):
        return row[0] if not isinstance(row, dict) else row["id"]

    def _insert_minimal_sensor(self, cur, name="test-sensor"):
        cur.execute(
            """
            INSERT INTO sensorthings."Sensor"
                ("name", "description", "encodingType", "metadata")
            VALUES (%s, 'desc', 'application/pdf', 'http://meta')
            RETURNING id
            """,
            (name,),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_observed_property(self, cur, name="test-op"):
        cur.execute(
            """
            INSERT INTO sensorthings."ObservedProperty"
                ("name", "definition", "description")
            VALUES (%s, 'http://def', 'desc')
            RETURNING id
            """,
            (name,),
        )
        return self._get_id(cur.fetchone())

    def _insert_minimal_datastream(self, cur, thing_id, sensor_id, op_id, name="test-ds"):
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

    def _insert_minimal_foi(self, cur, name="test-foi"):
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

    def _insert_observation(self, cur, ds_id, foi_id, result_type, **kwargs):
        """Insert one observation row; caller sets the correct result column."""
        col_map = {
            0: ("resultNumber", kwargs.get("resultNumber")),
            1: ("resultBoolean", kwargs.get("resultBoolean")),
            2: ("resultJSON", psycopg2.extras.Json(kwargs.get("resultJSON")) if kwargs.get("resultJSON") is not None else None),
            3: ("resultString", kwargs.get("resultString")),
            99: ("resultNumber", None),
        }
        col, val = col_map[result_type]
        cur.execute(
            f"""
            INSERT INTO sensorthings."Observation"
                ("resultType", "{col}", "datastream_id", "featuresofinterest_id")
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (result_type, val, ds_id, foi_id),
        )
        return self._get_id(cur.fetchone())

    def _get_result(self, cur, obs_id):
        cur.execute(
            """
            SELECT result(o)
            FROM sensorthings."Observation" o
            WHERE id = %s
            """,
            (obs_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _setup_ds_foi(self, cur):
        """Common pre-requisites needed by Observation tests."""
        thing_id = self._insert_minimal_thing(cur, "t-obs")
        sensor_id = self._insert_minimal_sensor(cur, "s-obs")
        op_id = self._insert_minimal_observed_property(cur, "op-obs")
        ds_id = self._insert_minimal_datastream(cur, thing_id, sensor_id, op_id, "ds-obs")
        foi_id = self._insert_minimal_foi(cur, "foi-obs")
        return ds_id, foi_id

    # -------------------------------------------------------------------------
    # 1. result() dispatch
    # -------------------------------------------------------------------------

    def test_result_type_0_number(self, schema):
        """resultType 0 -> to_jsonb(resultNumber) round-trips correctly."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 0, resultNumber=42.5)
            result = self._get_result(cur, obs_id)
        assert result == 42.5

    def test_result_type_1_boolean_true(self, schema):
        """resultType 1 -> to_jsonb(resultBoolean) returns True."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 1, resultBoolean=True)
            result = self._get_result(cur, obs_id)
        assert result is True

    def test_result_type_1_boolean_false(self, schema):
        """resultType 1 -> to_jsonb(resultBoolean) returns False."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 1, resultBoolean=False)
            result = self._get_result(cur, obs_id)
        assert result is False

    def test_result_type_2_json(self, schema):
        """resultType 2 -> resultJSON is returned as-is (jsonb passthrough)."""
        payload = {"sensor": "temp", "unit": "C"}
        with schema.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 2, resultJSON=payload)
        with schema.cursor() as cur:
            result = self._get_result(cur, obs_id)
        assert result == payload

    def test_result_type_3_string(self, schema):
        """resultType 3 -> to_jsonb(resultString) returns the string as a JSON string."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 3, resultString="hello")
            result = self._get_result(cur, obs_id)
        assert result == "hello"

    def test_result_type_invalid_returns_null(self, schema):
        """resultType outside 0-3 -> ELSE branch returns NULL."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 99)
            result = self._get_result(cur, obs_id)
        assert result is None

    # -------------------------------------------------------------------------
    # 2. delete_related_historical_locations trigger
    # -------------------------------------------------------------------------

    def test_delete_location_cascades_historical_location(self, schema):
        """
        Deleting a Location that is linked to a Thing via Thing_Location
        must fire the BEFORE DELETE trigger and remove the HistoricalLocation
        associated with that Thing.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "hl-thing")
            loc_id = self._insert_minimal_location(cur, "hl-loc")

            cur.execute(
                """
                INSERT INTO sensorthings."Thing_Location"
                    ("thing_id", "location_id")
                VALUES (%s, %s)
                """,
                (thing_id, loc_id),
            )

            cur.execute(
                """
                INSERT INTO sensorthings."HistoricalLocation"
                    ("thing_id")
                VALUES (%s)
                RETURNING id
                """,
                (thing_id,),
            )
            hl_id = cur.fetchone()[0]

            cur.execute(
                'DELETE FROM sensorthings."Location" WHERE id = %s',
                (loc_id,),
            )

            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl_id,),
            )
            row = cur.fetchone()

        assert row is None, (
            f"HistoricalLocation {hl_id} should have been deleted by the trigger"
        )

    def test_delete_location_not_linked_does_not_affect_other_hl(self, schema):
        """
        Deleting an unlinked Location must not delete HistoricalLocations
        belonging to unrelated Things.
        """
        with schema.cursor() as cur:
            thing_id = self._insert_minimal_thing(cur, "safe-thing")
            unrelated_loc_id = self._insert_minimal_location(cur, "unrelated-loc")

            cur.execute(
                """
                INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                VALUES (%s)
                RETURNING id
                """,
                (thing_id,),
            )
            hl_id = cur.fetchone()[0]

            cur.execute(
                'DELETE FROM sensorthings."Location" WHERE id = %s',
                (unrelated_loc_id,),
            )

            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl_id,),
            )
            row = cur.fetchone()

        assert row is not None, (
            "HistoricalLocation for an unrelated Thing must survive the Location delete"
        )
