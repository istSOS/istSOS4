"""Exercise DataArray ingestion without a running database."""

import asyncio
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from app.v1.endpoints.create import data_array_observation as endpoint


@pytest.fixture
def connection(monkeypatch):
    """Record SQL calls while returning a stable Datastream/Location fixture."""
    conn = AsyncMock()
    conn.inserted = []
    conn.location = {
        "id": 1,
        "name": "Station",
        "description": "Synthetic station",
        "encodingType": "application/geo+json",
        "location": '{"type":"Point","coordinates":[0,0]}',
        "properties": None,
        "gen_foi_id": 7,
    }
    conn.last_foi_id = 7

    @asynccontextmanager
    async def transaction():
        yield

    conn.transaction = transaction

    async def fetchval(query, *args):
        if '"observationType"' in query:
            return "OM_Measurement"
        if "SELECT last_foi_id" in query:
            return conn.last_foi_id
        if 'INSERT INTO sensorthings."FeaturesOfInterest"' in query:
            return 7
        raise AssertionError(f"Unexpected fetchval: {query}")

    async def fetch(query, *args):
        if "ST_AsGeoJSON" in query:
            return [dict(conn.location)]
        if "SELECT id" in query:
            return [{"id": 1}]
        if 'INSERT INTO sensorthings."Observation"' in query:
            columns = re.findall(
                r'"([^"]+)"', query.split("(", 1)[1].split(")", 1)[0]
            )
            row = dict(zip(columns, args))
            row["id"] = len(conn.inserted) + 1
            row.setdefault("resultTime", None)
            conn.inserted.append(row)
            return [row]
        raise AssertionError(f"Unexpected fetch: {query}")

    async def execute(query, *args):
        if 'SET "gen_foi_id"' in query:
            conn.location["gen_foi_id"] = args[0]
        if "SET last_foi_id" in query:
            conn.last_foi_id = args[0]
        return "UPDATE 1"

    conn.fetchval.side_effect = fetchval
    conn.fetch.side_effect = fetch
    conn.execute.side_effect = execute
    monkeypatch.setattr(endpoint, "set_commit", AsyncMock(return_value=None))
    resolver = AsyncMock(wraps=endpoint.generate_feature_of_interest)
    monkeypatch.setattr(endpoint, "generate_feature_of_interest", resolver)
    return conn


def create_rows(connection, results, explicit_foi=None):
    @asynccontextmanager
    async def acquire():
        yield connection

    class Pool:
        pass

    pool = Pool()
    pool.acquire = acquire
    components = ["phenomenonTime", "result"]
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [
        [(start + timedelta(seconds=i)).isoformat(), value]
        for i, value in enumerate(results)
    ]
    if explicit_foi is not None:
        components.append("FeatureOfInterest/id")
        for row in rows:
            row.append(explicit_foi)
    return asyncio.run(
        endpoint.data_array_observation(
            payload=[
                {
                    "Datastream": {"@iot.id": 1},
                    "components": components,
                    "dataArray": rows,
                }
            ],
            commit_message=None,
            current_user=None,
            pool=pool,
        )
    )


@pytest.mark.parametrize("size", [1, 10, 100])
def test_resolves_feature_once_per_row(connection, size):
    response = create_rows(connection, range(size))

    assert response.status_code == 201
    assert json.loads(response.body) == [
        endpoint.build_self_link("Observation", i + 1) for i in range(size)
    ]
    assert [row["resultNumber"] for row in connection.inserted] == list(
        range(size)
    )
    assert all(
        row["featuresofinterest_id"] == 7 for row in connection.inserted
    )
    assert endpoint.generate_feature_of_interest.await_count == size
    # One type lookup per set, then three FoI SELECTs, INSERT and range UPDATE
    # per row. Transaction-control statements are not included.
    assert (
        connection.fetch.await_count
        + connection.fetchval.await_count
        + connection.execute.await_count
    ) == 5 * size + 1


def test_first_feature_is_generated_and_reused(connection):
    connection.location["gen_foi_id"] = None
    connection.last_foi_id = None

    response = create_rows(connection, [1, 2])

    assert response.status_code == 201
    assert "error" not in json.loads(response.body)
    assert endpoint.generate_feature_of_interest.await_count == 2
    assert connection.location["gen_foi_id"] == 7
    assert all(
        row["featuresofinterest_id"] == 7 for row in connection.inserted
    )
    feature_inserts = [
        call
        for call in connection.fetchval.await_args_list
        if 'INSERT INTO sensorthings."FeaturesOfInterest"' in call.args[0]
    ]
    assert len(feature_inserts) == 1


def test_explicit_feature_bypasses_generation(connection):
    response = create_rows(connection, [1, 2], explicit_foi=8)

    assert response.status_code == 201
    assert "error" not in json.loads(response.body)
    endpoint.generate_feature_of_interest.assert_not_awaited()
    assert [row["featuresofinterest_id"] for row in connection.inserted] == [
        8,
        8,
    ]
    assert connection.last_foi_id == 8


def test_invalid_row_keeps_response_order_and_later_rows(connection):
    response = create_rows(connection, [1, "invalid", 3])

    assert response.status_code == 201
    assert json.loads(response.body) == [
        endpoint.build_self_link("Observation", 1),
        "error",
        endpoint.build_self_link("Observation", 2),
    ]
    assert [row["resultNumber"] for row in connection.inserted] == [1, 3]
    assert endpoint.generate_feature_of_interest.await_count == 3


@pytest.mark.parametrize("resolved_id", ["missing", None, 7])
def test_helper_resolves_only_missing_feature(connection, resolved_id):
    payload = {
        "datastream_id": 1,
        "phenomenonTime": "2025-01-01T00:00:00Z",
        "result": 1,
    }
    if resolved_id != "missing":
        payload["featuresofinterest_id"] = resolved_id

    asyncio.run(
        endpoint.insertDataArrayObservation(
            payload, connection, observation_type="OM_Measurement"
        )
    )

    assert connection.inserted[0]["featuresofinterest_id"] == 7
    assert endpoint.generate_feature_of_interest.await_count == (
        0 if resolved_id == 7 else 1
    )
