"""
test(schema): psycopg2 integration tests for istsos_schema.sql

Tests:
  1. result() dispatch on Observation for all resultType variants (0-3 + invalid)
  2. delete_related_historical_locations trigger cascade
  3. @iot.selfLink computed functions for all main entities
  4. expand() nextLink pagination sentinel (present vs absent)

Run from repo root:  pytest test/database/test_schema.py -v
Run from test/:      pytest database/test_schema.py -v
"""

import json
import psycopg2
import psycopg2.extras
import pytest

from test.database.conftest import (
    recreate_database,
    get_raw_conn,
    make_dsn,
    load_base_schema,
    insert_minimal_thing,
    insert_minimal_location,
    insert_minimal_sensor,
    insert_minimal_observed_property,
    insert_minimal_datastream,
    insert_minimal_foi,
    get_id,
)

TEST_DB = "istsos_test"
DSN = make_dsn(TEST_DB)

# Static SQL strings for each Observation resultType.
# Kept per-type so no column name is ever injected dynamically.
_INSERT_OBS_SQL = {
    0: """
        INSERT INTO sensorthings."Observation"
            ("resultType", "resultNumber", "datastream_id", "featuresofinterest_id")
        VALUES (%s, %s, %s, %s) RETURNING id
       """,
    1: """
        INSERT INTO sensorthings."Observation"
            ("resultType", "resultBoolean", "datastream_id", "featuresofinterest_id")
        VALUES (%s, %s, %s, %s) RETURNING id
       """,
    2: """
        INSERT INTO sensorthings."Observation"
            ("resultType", "resultJSON", "datastream_id", "featuresofinterest_id")
        VALUES (%s, %s, %s, %s) RETURNING id
       """,
    3: """
        INSERT INTO sensorthings."Observation"
            ("resultType", "resultString", "datastream_id", "featuresofinterest_id")
        VALUES (%s, %s, %s, %s) RETURNING id
       """,
}


class TestSchema:
    """
    Integration tests for the base istsos_schema.sql.

    The schema is loaded once per class into a fresh database.
    Each test runs inside a transaction that is rolled back by the
    auto_rollback fixture in conftest.py, so tests are fully isolated.
    """

    @pytest.fixture(autouse=True, scope="class")
    def schema(self):
        """Recreate the test database, load the base schema, yield the connection."""
        recreate_database(TEST_DB)

        setup_conn = get_raw_conn(DSN)
        load_base_schema(setup_conn)
        setup_conn.close()

        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        yield conn
        conn.close()

    @pytest.fixture(autouse=True)
    def rollback(self, schema):
        yield
        schema.rollback()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _setup_ds_foi(self, cur, suffix="obs"):
        """Insert the full dependency chain needed for Observation tests."""
        thing_id  = insert_minimal_thing(cur, f"t-{suffix}")
        sensor_id = insert_minimal_sensor(cur, f"s-{suffix}")
        op_id     = insert_minimal_observed_property(cur, f"op-{suffix}")
        ds_id     = insert_minimal_datastream(cur, thing_id, sensor_id, op_id, f"ds-{suffix}")
        foi_id    = insert_minimal_foi(cur, f"foi-{suffix}")
        return ds_id, foi_id

    def _insert_observation(self, cur, ds_id, foi_id, result_type, **kwargs):
        """Insert one Observation row using a static SQL string per resultType."""
        value_map = {
            0: kwargs.get("resultNumber"),
            1: kwargs.get("resultBoolean"),
            2: psycopg2.extras.Json(kwargs["resultJSON"])
               if kwargs.get("resultJSON") is not None else None,
            3: kwargs.get("resultString"),
        }
        sql = _INSERT_OBS_SQL.get(result_type)
        if sql is None:
            # resultType outside 0-3: exercise the ELSE branch of result()
            cur.execute(
                """
                INSERT INTO sensorthings."Observation"
                    ("resultType", "resultNumber", "datastream_id", "featuresofinterest_id")
                VALUES (%s, NULL, %s, %s) RETURNING id
                """,
                (result_type, ds_id, foi_id),
            )
        else:
            cur.execute(sql, (result_type, value_map[result_type], ds_id, foi_id))
        return get_id(cur.fetchone())

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

    # ------------------------------------------------------------------
    # 1. result() dispatch
    # ------------------------------------------------------------------

    def test_result_type_0_number(self, schema):
        """resultType 0 -> to_jsonb(resultNumber) round-trips correctly."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 0, resultNumber=42.5)
            result = self._get_result(cur, obs_id)
        assert result == 42.5

    def test_result_type_0_null_value(self, schema):
        """resultType 0 with NULL resultNumber -> returns SQL NULL (Python None)."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            cur.execute(
                """
                INSERT INTO sensorthings."Observation"
                    ("resultType", "resultNumber", "datastream_id", "featuresofinterest_id")
                VALUES (0, NULL, %s, %s) RETURNING id
                """,
                (ds_id, foi_id),
            )
            obs_id = cur.fetchone()[0]
            result = self._get_result(cur, obs_id)
        assert result is None

    @pytest.mark.parametrize("value", [True, False])
    def test_result_type_1_boolean(self, schema, value):
        """resultType 1 -> to_jsonb(resultBoolean) round-trips for both values."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 1, resultBoolean=value)
            result = self._get_result(cur, obs_id)
        assert result is value

    def test_result_type_2_json(self, schema):
        """resultType 2 -> resultJSON is returned as-is (jsonb passthrough)."""
        payload = {"sensor": "temp", "unit": "C"}
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 2, resultJSON=payload)
            result = self._get_result(cur, obs_id)
        assert result == payload

    def test_result_type_3_string(self, schema):
        """resultType 3 -> to_jsonb(resultString) returns the string."""
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

    # ------------------------------------------------------------------
    # 2. delete_related_historical_locations trigger
    # ------------------------------------------------------------------

    def test_delete_location_cascades_historical_location(self, schema):
        """
        Deleting a Location linked to a Thing via Thing_Location must fire the
        BEFORE DELETE trigger and remove the associated HistoricalLocation.
        """
        with schema.cursor() as cur:
            thing_id = insert_minimal_thing(cur, "hl-thing")
            loc_id   = insert_minimal_location(cur, "hl-loc")

            cur.execute(
                """
                INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                VALUES (%s, %s)
                """,
                (thing_id, loc_id),
            )
            cur.execute(
                """
                INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                VALUES (%s) RETURNING id
                """,
                (thing_id,),
            )
            hl_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO sensorthings."Location_HistoricalLocation"
                    ("location_id", "historicallocation_id")
                VALUES (%s, %s)
                """,
                (loc_id, hl_id),
            )
            cur.execute('DELETE FROM sensorthings."Location" WHERE id = %s', (loc_id,))
            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl_id,),
            )
            row = cur.fetchone()

        assert row is None, (
            f"HistoricalLocation {hl_id} should have been deleted by the trigger"
        )

    def test_delete_location_not_linked_does_not_affect_other_hl(self, schema):
        """Deleting an unlinked Location must not touch unrelated HistoricalLocations."""
        with schema.cursor() as cur:
            thing_id       = insert_minimal_thing(cur, "safe-thing")
            unrelated_loc  = insert_minimal_location(cur, "unrelated-loc")

            cur.execute(
                """
                INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                VALUES (%s) RETURNING id
                """,
                (thing_id,),
            )
            hl_id = cur.fetchone()[0]

            cur.execute('DELETE FROM sensorthings."Location" WHERE id = %s', (unrelated_loc,))
            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl_id,),
            )
            row = cur.fetchone()

        assert row is not None, (
            "HistoricalLocation for an unrelated Thing must survive the Location delete"
        )

    def test_delete_location_cascades_via_join_table(self, schema):
        """Trigger must follow the Location -> Thing_Location -> HistoricalLocation path."""
        with schema.cursor() as cur:
            thing_id = insert_minimal_thing(cur, "hl2-thing")
            loc_id   = insert_minimal_location(cur, "hl2-loc")

            cur.execute(
                """
                INSERT INTO sensorthings."Thing_Location" (thing_id, location_id)
                VALUES (%s, %s)
                """,
                (thing_id, loc_id),
            )
            cur.execute(
                """
                INSERT INTO sensorthings."HistoricalLocation" (thing_id)
                VALUES (%s) RETURNING id
                """,
                (thing_id,),
            )
            hl_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO sensorthings."Location_HistoricalLocation"
                    (location_id, historicallocation_id)
                VALUES (%s, %s)
                """,
                (loc_id, hl_id),
            )
            cur.execute('DELETE FROM sensorthings."Location" WHERE id = %s', (loc_id,))
            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl_id,),
            )
            assert cur.fetchone() is None

    def test_delete_location_only_cascades_linked_thing(self, schema):
        """
        Two Things sharing one Location: deleting that Location must cascade
        HLs for both linked Things and leave the unrelated Thing's HL intact.
        Previously raised CardinalityViolation when the trigger looped naively.
        """
        with schema.cursor() as cur:
            thing1_id  = insert_minimal_thing(cur, "shared-t1")
            thing2_id  = insert_minimal_thing(cur, "shared-t2")
            thing3_id  = insert_minimal_thing(cur, "unrelated-t3")
            loc_id     = insert_minimal_location(cur, "shared-loc")
            other_loc  = insert_minimal_location(cur, "other-loc")

            for tid in (thing1_id, thing2_id):
                cur.execute(
                    """
                    INSERT INTO sensorthings."Thing_Location" (thing_id, location_id)
                    VALUES (%s, %s)
                    """,
                    (tid, loc_id),
                )
            cur.execute(
                """
                INSERT INTO sensorthings."Thing_Location" (thing_id, location_id)
                VALUES (%s, %s)
                """,
                (thing3_id, other_loc),
            )

            hl_ids = []
            for tid in (thing1_id, thing2_id, thing3_id):
                cur.execute(
                    'INSERT INTO sensorthings."HistoricalLocation" (thing_id) '
                    'VALUES (%s) RETURNING id',
                    (tid,),
                )
                hl_ids.append(cur.fetchone()[0])
            hl1_id, hl2_id, hl3_id = hl_ids

            cur.execute('DELETE FROM sensorthings."Location" WHERE id = %s', (loc_id,))

            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = ANY(%s)',
                ([hl1_id, hl2_id],),
            )
            assert cur.fetchall() == [], "HLs for linked Things should be deleted"

            cur.execute(
                'SELECT id FROM sensorthings."HistoricalLocation" WHERE id = %s',
                (hl3_id,),
            )
            assert cur.fetchone() is not None, "HL for unrelated Thing must not be deleted"

    # ------------------------------------------------------------------
    # 3. @iot.selfLink computed functions
    # ------------------------------------------------------------------

    def test_selflink_thing(self, schema):
        """@iot.selfLink for Thing must return '/Things(<id>)'."""
        with schema.cursor() as cur:
            thing_id = insert_minimal_thing(cur, "sl-thing")
            cur.execute(
                'SELECT "@iot.selfLink"(t) FROM sensorthings."Thing" t WHERE id = %s',
                (thing_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Things({thing_id})"

    def test_selflink_location(self, schema):
        """@iot.selfLink for Location must return '/Locations(<id>)'."""
        with schema.cursor() as cur:
            loc_id = insert_minimal_location(cur, "sl-loc")
            cur.execute(
                'SELECT "@iot.selfLink"(l) FROM sensorthings."Location" l WHERE id = %s',
                (loc_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Locations({loc_id})"

    def test_selflink_sensor(self, schema):
        """@iot.selfLink for Sensor must return '/Sensors(<id>)'."""
        with schema.cursor() as cur:
            sensor_id = insert_minimal_sensor(cur, "sl-sensor")
            cur.execute(
                'SELECT "@iot.selfLink"(s) FROM sensorthings."Sensor" s WHERE id = %s',
                (sensor_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Sensors({sensor_id})"

    def test_selflink_observed_property(self, schema):
        """@iot.selfLink for ObservedProperty must return '/ObservedProperties(<id>)'."""
        with schema.cursor() as cur:
            op_id = insert_minimal_observed_property(cur, "sl-op")
            cur.execute(
                'SELECT "@iot.selfLink"(op) FROM sensorthings."ObservedProperty" op WHERE id = %s',
                (op_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/ObservedProperties({op_id})"

    def test_selflink_datastream(self, schema):
        """@iot.selfLink for Datastream must return '/Datastreams(<id>)'."""
        with schema.cursor() as cur:
            ds_id, _ = self._setup_ds_foi(cur, suffix="sl-ds")
            cur.execute(
                'SELECT "@iot.selfLink"(d) FROM sensorthings."Datastream" d WHERE id = %s',
                (ds_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Datastreams({ds_id})"

    def test_selflink_features_of_interest(self, schema):
        """@iot.selfLink for FeaturesOfInterest must return '/FeaturesOfInterest(<id>)'."""
        with schema.cursor() as cur:
            foi_id = insert_minimal_foi(cur, "sl-foi")
            cur.execute(
                'SELECT "@iot.selfLink"(f) FROM sensorthings."FeaturesOfInterest" f WHERE id = %s',
                (foi_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/FeaturesOfInterest({foi_id})"

    def test_selflink_observation(self, schema):
        """@iot.selfLink for Observation must return '/Observations(<id>)'."""
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur)
            obs_id = self._insert_observation(cur, ds_id, foi_id, 3, resultString="x")
            cur.execute(
                'SELECT "@iot.selfLink"(o) FROM sensorthings."Observation" o WHERE id = %s',
                (obs_id,),
            )
            link = cur.fetchone()[0]
        assert link == f"/Observations({obs_id})"

    # ------------------------------------------------------------------
    # 4. expand() nextLink pagination sentinel
    # ------------------------------------------------------------------

    def test_expand_nextlink_present_when_overflow(self, schema):
        """
        When the number of matching rows exceeds limit_ - 1, expand() must
        return a non-null @iot.nextLink.

        We insert 3 Observations against one Datastream and call expand()
        with limit=3, so limit-1=2 < 3 rows -> overflow expected.
        """
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur, suffix="pg")

            for i in range(3):
                cur.execute(
                    """
                    INSERT INTO sensorthings."Observation"
                        ("resultType", "resultString", "datastream_id",
                         "featuresofinterest_id", "phenomenonTime")
                    VALUES (%s, %s, %s, %s,
                        tstzrange(
                            now() + (%s || ' seconds')::interval,
                            now() + (%s || ' seconds')::interval,
                            '[]'
                        ))
                    """,
                    (3, f"obs-{i}", ds_id, foi_id, i, i),
                )

            cur.execute(
                """
                SELECT sensorthings.expand(
                    %s, 'datastream_id', %s,
                    3, 0, true, false, 'Observation', '', false
                )
                """,
                ('SELECT * FROM sensorthings."Observation"', ds_id),
            )
            result = cur.fetchone()[0]

        if isinstance(result, str):
            result = json.loads(result)

        assert result.get("Observation@iot.nextLink") is not None, (
            "expand() should produce a non-null @iot.nextLink when rows exceed limit_-1"
        )

    def test_expand_nextlink_null_when_under_limit(self, schema):
        """
        When the number of matching rows is below limit_ - 1, expand() must
        return null for @iot.nextLink.
        """
        with schema.cursor() as cur:
            ds_id, foi_id = self._setup_ds_foi(cur, suffix="pg2")
            self._insert_observation(cur, ds_id, foi_id, 3, resultString="only-one")

            cur.execute(
                """
                SELECT sensorthings.expand(
                    %s, 'datastream_id', %s,
                    10, 0, true, false, 'Observation', '', false
                )
                """,
                ('SELECT * FROM sensorthings."Observation"', ds_id),
            )
            result = cur.fetchone()[0]

        if isinstance(result, str):
            result = json.loads(result)

        assert result.get("Observation@iot.nextLink") is None, (
            "expand() should return null @iot.nextLink when row count < limit_-1"
        )