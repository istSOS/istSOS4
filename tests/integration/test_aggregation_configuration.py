"""Real PostgreSQL + HTTP tests, using disposable databases only.

Set AGGREGATION_TEST_DSN to a disposable PostgreSQL/TimescaleDB server whose
login can create/drop databases and roles. No existing database is modified.
Each mode gets a uniquely named database loaded from the actual schema files.
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest
from app.v1.endpoints import aggregation_configuration as configurations
from app.v1.endpoints.exception_handlers import register_exception_handlers
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
ADMIN_DSN = os.getenv("AGGREGATION_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not ADMIN_DSN,
    reason="Set AGGREGATION_TEST_DSN to a disposable test server",
)


async def execute(database, sql, *args, fetch=False):
    connection = await asyncpg.connect(ADMIN_DSN, database=database)
    try:
        method = connection.fetch if fetch else connection.execute
        return await method(sql, *args)
    finally:
        await connection.close()


@pytest.fixture(
    scope="module",
    params=[False, True],
    ids=["unversioned-duplicates-allowed", "versioned"],
)
def store(request):
    versioned = request.param
    name = "agg_config_test_" + uuid.uuid4().hex[:12]
    login = name + "_admin"

    async def setup():
        admin = await asyncpg.connect(ADMIN_DSN)
        try:
            await admin.execute(f'CREATE DATABASE "{name}";')
            existing_role = await admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='administrator')"
            )
        finally:
            await admin.close()
        connection = await asyncpg.connect(
            ADMIN_DSN,
            database=name,
            server_settings={
                "custom.versioning": str(int(versioned)),
                "custom.authorization": "0",
                "custom.network": "0",
                "custom.duplicates": str(int(not versioned)),
                "custom.epsg": "4326",
                "custom.user": login,
                "custom.password": "disposable-test-only",
            },
        )
        try:
            schema = (ROOT / "database/istsos_schema.sql").read_text()
            # PostgreSQL roles are cluster-wide; the schema is database-local.
            if existing_role:
                schema = schema.replace(
                    'CREATE ROLE "administrator" WITH CREATEROLE;', ""
                )
            await connection.execute(schema)
            for filename in (
                "istsos_auth.sql",
                "istsos_schema_versioning.sql",
                "aggregation_configuration.sql",
                "aggregation_results.sql",
                "aggregation_processing_state.sql",
                "aggregation_engine.sql",
            ):
                await connection.execute((ROOT / "database" / filename).read_text())
            # Verify that the installation scripts are idempotent as documented.
            await connection.execute(
                (ROOT / "database/aggregation_results.sql").read_text()
            )
            await connection.execute(
                (ROOT / "database/aggregation_configuration.sql").read_text()
            )
            if versioned:
                await connection.execute("""INSERT INTO sensorthings."Commit" (id, author, message, "actionType")
                    VALUES (1, 'test', 'Seed configurations', 'CREATE');""")
            cols = ", commit_id" if versioned else ""
            vals = ", 1" if versioned else ""
            await connection.execute(f"""
                INSERT INTO sensorthings."Thing" (id, name, description{cols}) VALUES (1, 'test', 'test'{vals});
                INSERT INTO sensorthings."Sensor" (id, name, description, "encodingType", metadata{cols})
                    VALUES (1, 'test', 'test', 'text/plain', 'test'{vals});
                INSERT INTO sensorthings."ObservedProperty" (id, name, definition, description{cols})
                    VALUES (1, 'test', 'test', 'test'{vals});
            """)
        finally:
            await connection.close()

    async def cleanup():
        connection = await asyncpg.connect(ADMIN_DSN)
        try:
            await connection.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE);')
            await connection.execute(f'DROP ROLE IF EXISTS "{login}";')
        finally:
            await connection.close()

    try:
        asyncio.run(setup())
        yield name, versioned
    finally:
        asyncio.run(cleanup())


@pytest.fixture
def client(store):
    name, versioned = store
    cols = ", commit_id" if versioned else ""
    vals = ", 1" if versioned else ""
    asyncio.run(
        execute(
            name,
            f"""
        TRUNCATE aggregation.datastream_configuration RESTART IDENTITY CASCADE;
        TRUNCATE aggregation.observations_10min, aggregation.observations_1hour, aggregation.observations_1day, aggregation.observations_7days, aggregation.observations_1month, aggregation.observations_1year;
        INSERT INTO sensorthings."Datastream"
            (id, name, description, "unitOfMeasurement", "observationType",
             thing_id, sensor_id, observedproperty_id{cols})
        SELECT id, 'test-' || id, 'test', '{{}}'::jsonb, 'test', 1, 1, 1{vals}
        FROM generate_series(1, 3) id ON CONFLICT (id) DO NOTHING;
    """,
        )
    )

    @asynccontextmanager
    async def lifespan(application):
        application.state.pool = await asyncpg.create_pool(
            ADMIN_DSN, database=name, min_size=1, max_size=3
        )
        try:
            yield
        finally:
            await application.state.pool.close()

    application = FastAPI(lifespan=lifespan)
    register_exception_handlers(application)
    application.include_router(configurations.v1)

    async def get_test_pool():
        return application.state.pool

    application.dependency_overrides[configurations.get_pool] = get_test_pool
    application.dependency_overrides[configurations.get_pool_w] = get_test_pool
    with TestClient(application) as http:
        yield http


def post(client, **overrides):
    return client.post(
        "/AggregationConfigurations",
        json={"datastreamId": 1, "minInputQuality": 3, **overrides},
    )


def test_crud_preserves_overrides_and_deletes_only_configuration(client, store):
    before_jobs = asyncio.run(
        execute(
            store[0],
            "SELECT job_id FROM timescaledb_information.jobs ORDER BY job_id",
            fetch=True,
        )
    )
    created = post(
        client,
        minInputQuality=0,
        minCoverage=0.8,
        startTime="2026-01-01T00:00:00+01:00",
        endTime="2027-01-01T00:00:00+01:00",
        missingValues=[-999.9],
        conversionFactor=0.2,
    )
    assert created.status_code == 201, created.text
    document = created.json()
    path = f"/AggregationConfigurations({document['id']})"
    assert created.headers["location"] == document["@iot.selfLink"]
    assert document["Datastream@iot.navigationLink"].endswith("/Datastreams(1)")
    assert document["startTime"].startswith("2025-12-31T23:00:00")
    assert client.get(path).json() == document
    updated = client.patch(
        path,
        json={
            "enabled": False,
            "minInputQuality": 2,
            "conversionFactor": 1.0,
            "minCoverage": None,
            "endTime": None,
        },
    )
    assert updated.status_code == 200, updated.text
    changed = updated.json()
    assert changed["enabled"] is False
    assert changed["minInputQuality"] == 2
    assert changed["missingValues"] == [-999.9]
    assert changed["minCoverage"] is None
    assert "intervals" not in changed
    assert changed["endTime"] is None
    assert changed["createdAt"] == document["createdAt"]
    assert changed["updatedAt"] >= document["updatedAt"]
    assert client.delete(path).status_code == 204
    assert client.get(path).status_code == 404
    assert client.delete(path).status_code == 404
    assert asyncio.run(
        execute(
            store[0],
            'SELECT id FROM sensorthings."Datastream" WHERE id=1',
            fetch=True,
        )
    )
    after_jobs = asyncio.run(
        execute(
            store[0],
            "SELECT job_id FROM timescaledb_information.jobs ORDER BY job_id",
            fetch=True,
        )
    )
    assert before_jobs == after_jobs


def test_required_qc_reference_and_uniqueness(client):
    assert (
        client.post("/AggregationConfigurations", json={"datastreamId": 1}).status_code
        == 400
    )
    assert post(client, minInputQuality=None).status_code == 400
    assert post(client, datastreamId=999999).status_code == 400
    assert post(client).status_code == 201
    assert post(client).status_code == 409
    assert len(client.get("/AggregationConfigurations").json()["value"]) == 1


def test_patch_round_trips_every_processing_override(client):
    created = post(client).json()
    path = f"/AggregationConfigurations({created['id']})"
    settings = {
        "enabled": False,
        "mode": "sampled",
        "minInputQuality": 1,
        "ignoreQuality": True,
        "minCoverage": 0.75,
        "minObservations": 20,
        "maxConsecutiveMissing": 3,
        "conversionFactor": 0.2,
        "boundaryMode": "left_closed",
        "utcOffsetMinutes": 0,
        "bucketsToRecompute": 5,
        "startTime": "2026-01-01T00:00:00+00:00",
        "endTime": "2026-12-01T00:00:00+00:00",
        "resultQualityKey": "code",
        "missingValues": [-999.9, 999.9],
    }
    response = client.patch(path, json=settings)
    assert response.status_code == 200, response.text
    stored = client.get(path).json()
    assert all(stored[key] == value for key, value in settings.items())
    assert stored["datastreamId"] == 1
    assert stored["createdAt"] == created["createdAt"]

    overrides = {
        key: None
        for key in settings
        if key not in {"enabled", "mode", "minInputQuality", "ignoreQuality"}
    }
    response = client.patch(path, json=overrides)
    assert response.status_code == 200, response.text
    assert all(response.json()[key] is None for key in overrides)
    assert response.json()["minInputQuality"] == 1


def test_patch_validates_merged_dates_and_mode_without_partial_writes(client):
    doc = post(
        client,
        minCoverage=0.8,
        startTime="2026-01-01T00:00:00Z",
        endTime="2026-02-01T00:00:00Z",
    ).json()
    path = f"/AggregationConfigurations({doc['id']})"
    for patch in (
        {"startTime": "2026-03-01T00:00:00Z"},
        {"mode": "event"},
        {"minInputQuality": None},
        {"datastreamId": 2},
        {"id": 99},
        {},
    ):
        response = client.patch(path, json=patch)
        assert response.status_code == 400, response.text
        assert client.get(path).json() == doc
    response = client.patch(
        path,
        json={
            "mode": "event",
            "minCoverage": None,
            "startTime": None,
            "missingValues": [],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["mode"] == "event"


def test_list_filters_and_pagination_preserve_filters(client):
    for ds in (1, 2, 3):
        assert post(client, datastreamId=ds, enabled=ds != 2).status_code == 201
    page = client.get(
        "/AggregationConfigurations", params={"enabled": "true", "$top": 1}
    ).json()
    assert [row["datastreamId"] for row in page["value"]] == [1]
    # The app under test is not mounted under the deployment prefix.
    from urllib.parse import urlsplit

    next_url = urlsplit(page["@iot.nextLink"])
    page2 = client.get("/AggregationConfigurations?" + next_url.query).json()
    assert [row["datastreamId"] for row in page2["value"]] == [3]
    assert "@iot.nextLink" not in page2
    selected = client.get(
        "/AggregationConfigurations", params={"datastreamId": 2}
    ).json()
    assert [row["datastreamId"] for row in selected["value"]] == [2]
    assert (
        client.get("/AggregationConfigurations", params={"$top": 0}).status_code == 422
    )


def test_source_deletion_cascades_configuration(client, store):
    doc = post(client, datastreamId=2).json()
    asyncio.run(execute(store[0], 'DELETE FROM sensorthings."Datastream" WHERE id=2;'))
    assert client.get(f"/AggregationConfigurations({doc['id']})").status_code == 404


def test_all_routes_enforce_administrator_when_authorization_enabled(
    client, monkeypatch
):
    monkeypatch.setattr(configurations, "AUTHORIZATION", 1)
    for method, path, options in (
        ("get", "/AggregationConfigurations", {}),
        ("get", "/AggregationConfigurations(1)", {}),
        (
            "post",
            "/AggregationConfigurations",
            {"json": {"datastreamId": 1, "minInputQuality": 3}},
        ),
        (
            "patch",
            "/AggregationConfigurations(1)",
            {"json": {"enabled": False}},
        ),
        ("delete", "/AggregationConfigurations(1)", {}),
    ):
        response = getattr(client, method)(path, **options)
        assert response.status_code == 403, response.text


def test_missing_storage_returns_actionable_503(client, store):
    asyncio.run(
        execute(
            store[0],
            "ALTER TABLE aggregation.datastream_configuration RENAME TO configuration_test_hidden;",
        )
    )
    try:
        response = client.get("/AggregationConfigurations")
        assert response.status_code == 503, response.text
        assert "aggregation_configuration.sql" in response.text
    finally:
        asyncio.run(
            execute(
                store[0],
                "ALTER TABLE aggregation.configuration_test_hidden RENAME TO datastream_configuration;",
            )
        )


def test_database_constraints_protect_direct_writes(client, store):
    async def rejects():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            for values in ("1, 4", "999999, 3"):
                with pytest.raises(asyncpg.IntegrityConstraintViolationError):
                    await connection.execute(
                        f"INSERT INTO aggregation.datastream_configuration (datastream_id, min_input_quality) VALUES ({values})"
                    )
        finally:
            await connection.close()

    asyncio.run(rejects())


RESULT_TABLES = (
    "observations_10min",
    "observations_1hour",
    "observations_1day",
    "observations_7days",
    "observations_1month",
    "observations_1year",
)


def result_values(**overrides):
    from datetime import UTC, datetime, timedelta

    start = datetime(2026, 1, 1, tzinfo=UTC)
    return {
        "datastream_id": 1,
        "bucket_start": start,
        "bucket_end": start + timedelta(minutes=10),
        "evaluated_until": start + timedelta(minutes=5),
        "count_total": 3,
        "count_accepted": 2,
        "count_numeric": 2,
        "sum_value": 6.0,
        "min_value": 2.0,
        "max_value": 4.0,
        "avg_value": 3.0,
        "expected_count": 3,
        "coverage": 2 / 3,
        "result_quality": 63,
        "constraint_count": 0,
        "configuration_snapshot": '{"mode":"sampled","minInputQuality":3}',
        **overrides,
    }


async def insert_result(connection, table="observations_10min", **overrides):
    assert table in RESULT_TABLES
    values = result_values(**overrides)
    columns = ", ".join(values)
    params = ", ".join(f"${i}" for i in range(1, len(values) + 1))
    await connection.execute(
        f"INSERT INTO aggregation.{table} ({columns}) VALUES ({params})",
        *values.values(),
    )


def test_fixed_interval_configuration_rejects_overrides(client):
    assert post(client, intervals=["PT10M"]).status_code == 400
    created = post(client).json()
    assert "intervals" not in created
    assert (
        client.patch(
            f"/AggregationConfigurations({created['id']})",
            json={"intervals": None},
        ).status_code
        == 400
    )


def test_configuration_upgrade_preserves_rows_and_removes_interval_override(
    client, store
):
    created = post(client).json()
    asyncio.run(
        execute(
            store[0],
            """
        ALTER TABLE aggregation.datastream_configuration ADD COLUMN intervals TEXT[];
        UPDATE aggregation.datastream_configuration SET intervals=ARRAY['PT10M'];
    """,
        )
    )
    asyncio.run(
        execute(store[0], (ROOT / "database/aggregation_configuration.sql").read_text())
    )
    assert client.get(f"/AggregationConfigurations({created['id']})").json() == created


def test_six_shared_time_hypertables_have_no_refresh_jobs(client, store):
    async def check():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            names = await connection.fetch("""
                SELECT hypertable_name FROM timescaledb_information.hypertables
                WHERE hypertable_schema='aggregation' ORDER BY hypertable_name
            """)
            assert {row["hypertable_name"] for row in names} == set(RESULT_TABLES)
            dimensions = await connection.fetch("""
                SELECT hypertable_name, column_name, dimension_type
                FROM timescaledb_information.dimensions WHERE hypertable_schema='aggregation'
            """)
            assert len(dimensions) == 6
            assert all(
                row["column_name"] == "bucket_start" and row["dimension_type"] == "Time"
                for row in dimensions
            )
            assert (
                await connection.fetchval("""
                SELECT count(*) FROM timescaledb_information.continuous_aggregates
                WHERE view_schema='aggregation'
            """)
                == 0
            )
            before = await connection.fetch(
                "SELECT job_id FROM timescaledb_information.jobs ORDER BY job_id"
            )
            for table in RESULT_TABLES:
                await insert_result(connection, table)
                # Upsert replaces an open bucket without producing duplicates.
                await connection.execute(f"""
                    INSERT INTO aggregation.{table}
                    SELECT * FROM aggregation.{table} WHERE datastream_id=1
                    ON CONFLICT (datastream_id, bucket_start) DO UPDATE
                    SET sum_value=8, max_value=6, avg_value=4,
                        evaluated_until=EXCLUDED.bucket_end
                """)
            await connection.execute(
                (ROOT / "database/aggregation_results.sql").read_text()
            )
            for table in RESULT_TABLES:
                rows = await connection.fetch(f"SELECT * FROM aggregation.{table}")
                assert len(rows) == 1
                row = rows[0]
                assert row["sum_value"] == 8 and row["avg_value"] == 4
                assert row["evaluated_until"] == row["bucket_end"]
                assert len(row) == 17
            assert before == await connection.fetch(
                "SELECT job_id FROM timescaledb_information.jobs ORDER BY job_id"
            )
        finally:
            await connection.close()

    asyncio.run(check())


def test_result_retention_and_source_foreign_key(client, store):
    created = post(client).json()

    async def seed():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            # The application database role can write every result hypertable.
            await connection.execute("SET ROLE administrator")
            for table in RESULT_TABLES:
                await insert_result(connection, table)
                await insert_result(connection, table, datastream_id=2)
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await insert_result(connection, datastream_id=999999)
        finally:
            await connection.close()

    asyncio.run(seed())
    assert (
        client.delete(f"/AggregationConfigurations({created['id']})").status_code == 204
    )
    asyncio.run(execute(store[0], 'DELETE FROM sensorthings."Datastream" WHERE id=2'))
    for table in RESULT_TABLES:
        rows = asyncio.run(
            execute(
                store[0], f"SELECT datastream_id FROM aggregation.{table}", fetch=True
            )
        )
        assert [row["datastream_id"] for row in rows] == [1]


@pytest.mark.parametrize(
    "overrides",
    [
        {"count_total": -1},
        {"count_accepted": -1},
        {"count_accepted": 4},
        {"count_numeric": -1},
        {"count_numeric": 3},
        {"count_numeric": 0},
        {"sum_value": None},
        {"sum_value": float("nan")},
        {"avg_value": float("inf")},
        {"min_value": float("-inf")},
        {"max_value": 1.0},
        {"expected_count": -1},
        {"expected_count": None},
        {"expected_count": 0},
        {"coverage": None},
        {"coverage": -0.1},
        {"coverage": float("nan")},
        {"coverage": float("inf")},
        {"result_quality": -1},
        {"result_quality": 64},
        {"constraint_count": -1},
        {"constraint_count": 29},
        {"configuration_snapshot": "[]"},
        {"configuration_snapshot": "null"},
    ],
)
def test_result_constraints_reject_invalid_statistics(client, store, overrides):
    async def check():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            with pytest.raises(asyncpg.CheckViolationError):
                await insert_result(connection, **overrides)
        finally:
            await connection.close()

    asyncio.run(check())


def test_empty_bucket_and_oversampling_storage(client, store):
    async def check():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            await insert_result(
                connection,
                count_total=0,
                count_accepted=0,
                count_numeric=0,
                sum_value=None,
                min_value=None,
                max_value=None,
                avg_value=None,
                expected_count=None,
                coverage=None,
                result_quality=0,
            )
            await insert_result(
                connection, datastream_id=2, expected_count=1, coverage=2.0
            )
            await insert_result(
                connection,
                datastream_id=3,
                expected_count=0,
                coverage=None,
                constraint_count=28,
                result_quality=(1 << 62) - 1,
            )
            await connection.execute("DELETE FROM aggregation.observations_10min")
            for changes in (
                {"bucket_end": result_values()["bucket_start"]},
                {"evaluated_until": result_values()["bucket_end"].replace(hour=1)},
                {"evaluated_until": result_values()["bucket_start"].replace(year=2025)},
            ):
                with pytest.raises(asyncpg.CheckViolationError):
                    await insert_result(connection, **changes)
        finally:
            await connection.close()

    asyncio.run(check())


def test_ignore_quality_is_persisted_validated_and_reversible(client, store):
    created = post(client, minInputQuality=2).json()
    assert created["ignoreQuality"] is False
    path = f"/AggregationConfigurations({created['id']})"
    changed = client.patch(path, json={"ignoreQuality": True})
    assert changed.status_code == 200
    assert client.get(path).json()["ignoreQuality"] is True
    assert changed.json()["minInputQuality"] == 2
    for invalid in (None, 0, "true"):
        assert client.patch(path, json={"ignoreQuality": invalid}).status_code == 400
        assert client.get(path).json()["ignoreQuality"] is True
    assert (
        client.patch(path, json={"ignoreQuality": False}).json()["minInputQuality"] == 2
    )
    explicit = post(client, datastreamId=2, ignoreQuality=True)
    assert explicit.status_code == 201 and explicit.json()["ignoreQuality"] is True

    async def rejects_null():
        connection = await asyncpg.connect(ADMIN_DSN, database=store[0])
        try:
            with pytest.raises(asyncpg.NotNullViolationError):
                await connection.execute(
                    "UPDATE aggregation.datastream_configuration SET ignore_quality=NULL"
                )
        finally:
            await connection.close()

    asyncio.run(rejects_null())


def test_ignore_quality_upgrade_defaults_existing_rows_to_false(client, store):
    created = post(client, minInputQuality=2).json()
    asyncio.run(
        execute(
            store[0],
            "ALTER TABLE aggregation.datastream_configuration DROP COLUMN ignore_quality",
        )
    )
    script = (ROOT / "database/aggregation_configuration.sql").read_text()
    asyncio.run(execute(store[0], script))
    assert client.get(f"/AggregationConfigurations({created['id']})").json() == created
    client.patch(
        f"/AggregationConfigurations({created['id']})", json={"ignoreQuality": True}
    )
    asyncio.run(execute(store[0], script))
    assert (
        client.get(f"/AggregationConfigurations({created['id']})").json()[
            "ignoreQuality"
        ]
        is True
    )


def state_writer(store):
    from aggregation.state import ProcessingState

    return ProcessingState(conninfo=ADMIN_DSN, dbname=store[0])


def test_processing_state_lifecycle_is_read_only_and_preserves_checkpoint(
    client, store
):
    created = post(client).json()
    identifier = created["id"]
    path = f"/AggregationConfigurations({identifier})/ProcessingState"
    assert created["ProcessingState@iot.navigationLink"].endswith(path)
    initial = client.get(path).json()
    assert initial["status"] == "never_run" and initial["phase"] == "discovery"
    assert initial["checkpoint"] is None and initial["lastStartedAt"] is None
    for payload in ({"status": "success"}, {"processingState": {}}, {"checkpoint": {}}):
        assert (
            client.patch(
                f"/AggregationConfigurations({identifier})", json=payload
            ).status_code
            == 400
        )
    assert client.patch(path, json={"status": "success"}).status_code in (404, 405)
    writer = state_writer(store)
    writer.open()
    try:
        token = writer.start(identifier)
        running = client.get(path).json()
        assert running["status"] == "running" and running["lastFinishedAt"] is None
        writer.finish(identifier, token, "success")
        success = client.get(path).json()
        assert success["status"] == "success" and success["lastSuccessfulAt"]
        assert success["consecutiveFailures"] == 0
        asyncio.run(
            execute(
                store[0],
                "UPDATE aggregation.processing_state SET checkpoint='{}'::jsonb WHERE configuration_id=$1",
                identifier,
            )
        )
        for count in (1, 2):
            token = writer.start(identifier)
            writer.finish(
                identifier,
                token,
                "failed",
                {"step": "datastream_read", "message": "API returned HTTP 503"},
            )
            failed = client.get(path).json()
            assert (
                failed["status"] == "failed" and failed["consecutiveFailures"] == count
            )
            assert failed["lastError"]["step"] == "datastream_read"
            assert failed["lastSuccessfulAt"] == success["lastSuccessfulAt"]
            assert failed["checkpoint"] == {}
        writer.finish(identifier, writer.start(identifier), "success")
        final = client.get(path).json()
        assert final["lastError"] is None and final["consecutiveFailures"] == 0
        assert final["checkpoint"] == {}
    finally:
        writer.close()
    assert (
        client.get("/AggregationConfigurations(99999)/ProcessingState").status_code
        == 404
    )
    assert client.delete(f"/AggregationConfigurations({identifier})").status_code == 204
    assert not asyncio.run(
        execute(store[0], "SELECT * FROM aggregation.processing_state", fetch=True)
    )


def test_state_ownership_crash_recovery_and_stale_finish(client, store):
    from uuid import uuid4

    from aggregation.state import StateError

    identifier = post(client).json()["id"]
    path = f"/AggregationConfigurations({identifier})/ProcessingState"
    first = state_writer(store)
    second = state_writer(store)
    first.open()
    try:
        token = first.start(identifier)
        with pytest.raises(StateError, match="Another aggregation service"):
            second.open()
        first.finish(identifier, uuid4(), "success")
        assert client.get(path).json()["status"] == "running"
    finally:
        first.close()  # Simulate a crash before finishing.
    second.open()
    try:
        recovered = client.get(path).json()
        assert recovered["status"] == "interrupted"
        assert recovered["consecutiveFailures"] == 1
        assert recovered["lastError"]["step"] == "service"
        second.finish(identifier, token, "success")
        assert client.get(path).json()["status"] == "interrupted"
        assert second.start(999999) is None
        client.patch(
            f"/AggregationConfigurations({identifier})", json={"enabled": False}
        )
        assert second.start(identifier) is None
    finally:
        second.close()


def test_state_discovery_failure_partial_and_idempotent_install(client, store):
    first = post(client).json()["id"]
    second = post(client, datastreamId=2, enabled=False).json()["id"]
    writer = state_writer(store)
    writer.open()
    try:
        writer.discovery_failed("API request failed")
        assert (
            client.get(f"/AggregationConfigurations({first})/ProcessingState").json()[
                "status"
            ]
            == "failed"
        )
        assert (
            client.get(f"/AggregationConfigurations({second})/ProcessingState").json()[
                "status"
            ]
            == "never_run"
        )
        writer.finish(
            first,
            writer.start(first),
            "partial",
            {"step": "metadata_validation", "message": "Missing sampling frequency"},
        )
        before = client.get(
            f"/AggregationConfigurations({first})/ProcessingState"
        ).json()
        assert before["status"] == "partial" and before["lastSuccessfulAt"] is None
        assert before["consecutiveFailures"] == 2
        asyncio.run(
            execute(
                store[0],
                (ROOT / "database/aggregation_processing_state.sql").read_text(),
            )
        )
        assert (
            client.get(f"/AggregationConfigurations({first})/ProcessingState").json()
            == before
        )
    finally:
        writer.close()


def test_processing_state_endpoint_requires_admin_and_migration(
    client, store, monkeypatch
):
    identifier = post(client).json()["id"]
    path = f"/AggregationConfigurations({identifier})/ProcessingState"
    monkeypatch.setattr(configurations, "AUTHORIZATION", 1)
    assert client.get(path).status_code == 403
    monkeypatch.setattr(configurations, "AUTHORIZATION", 0)
    asyncio.run(
        execute(
            store[0],
            "ALTER TABLE aggregation.processing_state RENAME TO hidden_processing_state",
        )
    )
    try:
        response = client.get(path)
        assert (
            response.status_code == 503
            and "aggregation_processing_state.sql" in response.text
        )
    finally:
        asyncio.run(
            execute(
                store[0],
                "ALTER TABLE aggregation.hidden_processing_state RENAME TO processing_state",
            )
        )


def test_result_transaction_rolls_back_checkpoint_and_guards_config_edits(
    client, store
):
    from aggregation.calculation import CalculationError

    document = post(client).json()
    writer = state_writer(store)
    writer.open()
    try:
        token = writer.start(document["id"], phase="aggregation")
        row = result_values()
        row["configuration_snapshot"] = {"configuration": document}
        writer.save_buckets(
            document["id"], token, "observations_10min", [row], {"cursor": 1}
        )
        bad = {**row, "count_numeric": -1}
        with pytest.raises(CalculationError):
            writer.save_buckets(
                document["id"], token, "observations_10min", [bad], {"cursor": 2}
            )
        assert writer.checkpoint(document["id"]) == {"cursor": 1}
        saved = asyncio.run(
            execute(
                store[0],
                "SELECT count_numeric FROM aggregation.observations_10min",
                fetch=True,
            )
        )
        assert saved[0]["count_numeric"] == 2
        client.patch(
            f"/AggregationConfigurations({document['id']})", json={"minInputQuality": 2}
        )
        with pytest.raises(CalculationError, match="Configuration changed"):
            writer.save_buckets(
                document["id"], token, "observations_10min", [row], {"cursor": 3}
            )
        assert writer.checkpoint(document["id"]) == {"cursor": 1}
    finally:
        writer.close()


def test_version_watermark_stays_behind_open_transactions(client, store):
    from datetime import timedelta

    import psycopg

    writer = state_writer(store)
    writer.open()
    blocker = psycopg.connect(ADMIN_DSN, dbname=store[0])
    try:
        started = blocker.execute("SELECT transaction_timestamp()").fetchone()[0]
        watermark = writer.execute(
            "SELECT aggregation.safe_change_watermark() AS value", ()
        ).fetchone()["value"]
        assert watermark <= started - timedelta(seconds=1)
    finally:
        blocker.close()
        writer.close()


def test_worker_through_real_sensorthings_queries_and_pandas(
    client, store, monkeypatch, capsys
):
    from threading import Event
    from urllib.parse import urlsplit

    import app
    import requests
    from app.v1.endpoints.read.observation import v1 as observation_routes
    from app.v1.endpoints.read.read import v1 as read_routes

    from aggregation.calculation import timestamp
    from aggregation.service import SensorThings, Settings, run_cycle

    # The query models are constructed at import time. Run this test once with
    # VERSIONING=0 and once with VERSIONING=1 to exercise both actual schemas.
    if bool(app.VERSIONING) != store[1]:
        pytest.skip("Run with VERSIONING matching the database fixture")
    source_app = FastAPI()
    source_app.include_router(configurations.v1)
    source_app.include_router(observation_routes)
    source_app.include_router(read_routes)
    source_app.dependency_overrides.update(client.app.dependency_overrides)
    client.app.mount(app.SUBPATH + app.VERSION, source_app)
    versioned = store[1]
    columns, values = (",commit_id", ",1") if versioned else ("", "")
    asyncio.run(
        execute(
            store[0],
            f"""
        INSERT INTO sensorthings."FeaturesOfInterest"(id,name,description,"encodingType",feature{columns})
        VALUES (101,'agg-test','agg-test','application/geo+json','{{"type":"Point","coordinates":[0,0]}}'{values});
        UPDATE sensorthings."Datastream" SET properties='{{"samplingFrequency":"PT5M"}}' WHERE id=1;
        INSERT INTO sensorthings."Observation"("phenomenonTimeStart","phenomenonTimeEnd","resultType","resultNumber",datastream_id,featuresofinterest_id{columns})
        SELECT t,t,0,v,1,101{values} FROM (VALUES
            ('2026-01-01T00:05:00Z'::timestamptz,2.0),
            ('2026-01-01T00:10:00Z'::timestamptz,4.0),
            ('2026-01-01T00:15:00Z'::timestamptz,6.0)) AS inputs(t,v);
    """,
        )
    )
    if versioned:
        import time

        time.sleep(
            2.1
        )  # The stable API watermark intentionally trails active transactions.
    document = post(
        client,
        minInputQuality=0,
        bucketsToRecompute=1,
        utcOffsetMinutes=0,
        startTime="2026-01-01T00:01:00Z",
        endTime="2026-01-01T00:20:00Z",
    ).json()

    def request(session, method, url, **kwargs):
        parts = urlsplit(url)
        response = client.request(
            method,
            parts.path + ("?" + parts.query if parts.query else ""),
            headers=kwargs.get("headers"),
            data=kwargs.get("data"),
        )
        result = requests.Response()
        result.status_code, result._content, result.url = (
            response.status_code,
            response.content,
            url,
        )
        result.request = requests.Request(method, url).prepare()
        return result

    monkeypatch.setattr(requests.Session, "request", request)
    writer = state_writer(store)
    writer.open()
    settings = Settings("http://api" + app.SUBPATH + app.VERSION)
    api = SensorThings(settings, Event())
    try:
        assert run_cycle(api, settings, api.stop, writer, aggregate=True), (
            capsys.readouterr()
        )
        hourly = asyncio.run(
            execute(
                store[0],
                "SELECT sum_value,avg_value FROM aggregation.observations_1hour",
                fetch=True,
            )
        )
        assert hourly[0]["sum_value"] == 12 and hourly[0]["avg_value"] == 4
        state = client.get(
            f"/AggregationConfigurations({document['id']})/ProcessingState"
        ).json()
        assert state["phase"] == "aggregation" and state["status"] == "success"
        assert state["checkpoint"]["frontiers"]
        # Re-reading the same interval upserts rather than accumulating again.
        assert run_cycle(api, settings, api.stop, writer, aggregate=True)
        hourly = asyncio.run(
            execute(
                store[0],
                "SELECT sum_value FROM aggregation.observations_1hour",
                fetch=True,
            )
        )
        assert hourly[0]["sum_value"] == 12 and len(hourly) == 1
        if versioned:
            asyncio.run(
                execute(
                    store[0],
                    """
                DELETE FROM sensorthings."Observation" WHERE datastream_id=1 AND "resultNumber"=2;
                UPDATE sensorthings."Observation" SET "phenomenonTimeStart"='2026-01-01T00:17:00.5Z',
                    "phenomenonTimeEnd"='2026-01-01T00:17:00.5Z' WHERE datastream_id=1 AND "resultNumber"=4;
            """,
                )
            )
            time.sleep(2.1)
            assert run_cycle(api, settings, api.stop, writer, aggregate=True)
            buckets = asyncio.run(
                execute(
                    store[0],
                    "SELECT count_numeric,sum_value FROM aggregation.observations_10min ORDER BY bucket_start",
                    fetch=True,
                )
            )
            assert buckets[0]["count_numeric"] == 0 and buckets[0]["sum_value"] is None
            assert buckets[1]["sum_value"] == 10
            pages = list(
                api.observations(
                    1,
                    timestamp("2026-01-01T00:10:00Z"),
                    timestamp("2026-01-01T00:20:00Z"),
                    "gt",
                    "le",
                )
            )
            assert any(
                ".500000" in row["phenomenonTime"] for page in pages for row in page
            )
    finally:
        api.close()
        writer.close()


def test_phase_change_does_not_report_metadata_success_as_calculation_success(
    client, store
):
    identifier = post(client).json()["id"]
    path = f"/AggregationConfigurations({identifier})/ProcessingState"
    writer = state_writer(store)
    writer.open()
    try:
        writer.finish(identifier, writer.start(identifier), "success")
        assert client.get(path).json()["lastSuccessfulAt"]
        token = writer.start(identifier, phase="aggregation")
        running = client.get(path).json()
        assert running["phase"] == "aggregation" and running["lastSuccessfulAt"] is None
        writer.finish(
            identifier,
            token,
            "failed",
            {"step": "aggregation", "message": "Missing sampling frequency"},
        )
        writer.discovery_failed("API unavailable", phase="aggregation")
        failed = client.get(path).json()
        assert failed["phase"] == "aggregation" and failed["consecutiveFailures"] == 2
        assert failed["lastSuccessfulAt"] is None
    finally:
        writer.close()


def test_copy_bulk_upsert_is_atomic_and_reuses_empty_staging(client, store):
    from datetime import timedelta

    from aggregation.calculation import CalculationError

    document = post(client).json()
    writer = state_writer(store)
    writer.open()
    try:
        token = writer.start(document["id"], phase="aggregation")
        rows = []
        for i in range(25):
            row = result_values()
            for column in ("bucket_start", "bucket_end", "evaluated_until"):
                row[column] += timedelta(minutes=10 * i)
            row["configuration_snapshot"] = {"configuration": document}
            rows.append(row)
        writer.save_buckets(
            document["id"], token, "observations_10min", rows, {"cursor": 25}
        )
        assert (
            writer.execute(
                "SELECT count(*) AS n FROM aggregation.observations_10min", ()
            ).fetchone()["n"]
            == 25
        )
        assert (
            writer.execute(
                "SELECT count(*) AS n FROM pg_temp.aggregation_result_buffer", ()
            ).fetchone()["n"]
            == 0
        )
        # Existing rows and new rows share one COPY/merge transaction.
        updates = [{**row, "sum_value": 8.0, "avg_value": 4.0} for row in rows]
        new = {**rows[-1]}
        for column in ("bucket_start", "bucket_end", "evaluated_until"):
            new[column] += timedelta(minutes=10)
        writer.save_buckets(
            document["id"], token, "observations_10min", updates + [new], {"cursor": 26}
        )
        assert (
            writer.execute(
                "SELECT count(*) AS n FROM aggregation.observations_10min", ()
            ).fetchone()["n"]
            == 26
        )
        assert (
            writer.execute(
                "SELECT sum_value AS value FROM aggregation.observations_10min ORDER BY bucket_start LIMIT 1",
                (),
            ).fetchone()["value"]
            == 8
        )
        # One invalid row rolls back all updates, additions and checkpoint.
        rejected = [{**updates[0], "sum_value": 10.0}, {**new, "count_numeric": -1}]
        with pytest.raises(CalculationError):
            writer.save_buckets(
                document["id"], token, "observations_10min", rejected, {"cursor": 999}
            )
        assert writer.checkpoint(document["id"]) == {"cursor": 26}
        assert (
            writer.execute(
                "SELECT sum_value AS value FROM aggregation.observations_10min ORDER BY bucket_start LIMIT 1",
                (),
            ).fetchone()["value"]
            == 8
        )
        # Reuse the same staging table for another interval after failure.
        writer.save_buckets(
            document["id"], token, "observations_1hour", rows[:2], {"cursor": 27}
        )
        assert (
            writer.execute(
                "SELECT count(*) AS n FROM aggregation.observations_1hour", ()
            ).fetchone()["n"]
            == 2
        )
    finally:
        writer.close()


@pytest.mark.parametrize("invalid", [{"count_numeric": -1}, {"count_total": None}])
def test_failed_first_copy_batch_can_be_retried(client, store, invalid):
    from aggregation.calculation import CalculationError

    document = post(client).json()
    writer = state_writer(store)
    writer.open()
    try:
        token = writer.start(document["id"], phase="aggregation")
        row = result_values()
        row["configuration_snapshot"] = {"configuration": document}
        with pytest.raises(CalculationError):
            writer.save_buckets(
                document["id"],
                token,
                "observations_10min",
                [{**row, **invalid}],
                {"cursor": 1},
            )
        assert writer.checkpoint(document["id"]) is None
        writer.save_buckets(
            document["id"], token, "observations_10min", [row], {"cursor": 1}
        )
        assert writer.checkpoint(document["id"]) == {"cursor": 1}
    finally:
        writer.close()
