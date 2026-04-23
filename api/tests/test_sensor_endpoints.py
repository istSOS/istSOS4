import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ["DEBUG"] = "0"
os.environ["AUTHORIZATION"] = "0"
os.environ["REDIS"] = "0"
os.environ["VERSIONING"] = "0"
os.environ.setdefault("SECRET_KEY", "test_secret_key")

from app.v1.endpoints.create import sensor as create_sensor_ep  # noqa: E402
from app.v1.endpoints.delete import sensor as delete_sensor_ep  # noqa: E402
from app.v1.endpoints.read import sensor as read_sensor_ep  # noqa: E402
from app.v1.endpoints.update import sensor as update_sensor_ep  # noqa: E402


def _request(path="/Sensors", query_string=b"", headers=None):
    raw_headers = [
        (name.lower().encode(), value.encode())
        for name, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": query_string,
            "headers": raw_headers,
            "client": ("127.0.0.1", 1),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


def _pool_with_connection(connection):
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=None)
    connection.transaction = MagicMock(return_value=transaction)

    acquire = MagicMock()
    acquire.__aenter__ = AsyncMock(return_value=connection)
    acquire.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire)
    return pool


async def _stream_once(item):
    yield item


async def _empty_stream():
    if False:
        yield None


@pytest.mark.asyncio
async def test_get_sensors_streams_converted_sensor_query():
    request = _request("/Sensors", b"%24top=1")

    with patch.object(
        read_sensor_ep.sta2rest.STA2REST,
        "convert_query",
        return_value={
            "main_entity": "Sensor",
            "main_query": "SELECT * FROM sensorthings.\"Sensor\"",
            "top_value": 1,
            "is_count": False,
            "count_queries": [],
            "as_of_value": None,
            "from_to_value": False,
            "single_result": False,
        },
    ) as convert_query, patch.object(
        read_sensor_ep,
        "asyncpg_stream_results",
        return_value=_stream_once(b'{"value":[]}'),
    ) as stream_results:
        response = await read_sensor_ep.get_sensors(
            request=request,
            current_user=None,
            pool=MagicMock(),
            params=None,
        )

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    convert_query.assert_called_once_with("/Sensors?%24top=1")
    assert stream_results.call_args.args[:2] == (
        "Sensor",
        'SELECT * FROM sensorthings."Sensor"',
    )


@pytest.mark.asyncio
async def test_get_sensors_returns_404_when_stream_has_no_rows():
    request = _request("/Sensors")

    with patch.object(
        read_sensor_ep.sta2rest.STA2REST,
        "convert_query",
        return_value={
            "main_entity": "Sensor",
            "main_query": "SELECT 1",
            "top_value": None,
            "is_count": False,
            "count_queries": [],
            "as_of_value": None,
            "from_to_value": False,
            "single_result": False,
        },
    ), patch.object(
        read_sensor_ep,
        "asyncpg_stream_results",
        return_value=_empty_stream(),
    ):
        response = await read_sensor_ep.get_sensors(
            request=request,
            current_user=None,
            pool=MagicMock(),
            params=None,
        )

    assert response.status_code == 404
    assert response.body.decode() == (
        '{"code":404,"type":"error","message":"Not Found"}'
    )


@pytest.mark.asyncio
async def test_create_sensor_rejects_non_json_request_before_database_use():
    response = await create_sensor_ep.create_sensor(
        request=_request(headers={"content-type": "text/plain"}),
        payload={
            "name": "sensor name",
            "encodingType": "application/pdf",
            "metadata": "Light flux sensor",
        },
        commit_message=None,
        current_user=None,
        pool=MagicMock(),
    )

    assert response.status_code == 400
    assert response.body.decode() == (
        '{"code":400,"type":"error","message":"Only content-type application/json is supported."}'
    )


@pytest.mark.asyncio
async def test_create_sensor_returns_location_header_from_insert():
    connection = MagicMock()
    pool = _pool_with_connection(connection)
    payload = {
        "name": "sensor name",
        "encodingType": "application/pdf",
        "metadata": "Light flux sensor",
    }

    with patch.object(
        create_sensor_ep, "set_commit", AsyncMock(return_value=None)
    ), patch.object(
        create_sensor_ep,
        "insert_sensor_entity",
        AsyncMock(return_value=(1, "/Sensors(1)")),
    ) as insert_sensor:
        response = await create_sensor_ep.create_sensor(
            request=_request(headers={"content-type": "application/json"}),
            payload=payload,
            commit_message=None,
            current_user=None,
            pool=pool,
        )

    assert response.status_code == 201
    assert response.headers["location"] == "/Sensors(1)"
    insert_sensor.assert_awaited_once_with(connection, payload, None)


@pytest.mark.asyncio
async def test_update_sensor_returns_404_when_sensor_id_does_not_exist():
    connection = MagicMock()
    pool = _pool_with_connection(connection)

    with patch.object(
        update_sensor_ep, "check_id_exists", AsyncMock(return_value=False)
    ):
        response = await update_sensor_ep.update_sensor(
            sensor_id=42,
            payload={"name": "updated sensor"},
            commit_message=None,
            current_user=None,
            pool=pool,
        )

    assert response.status_code == 404
    assert response.body.decode() == (
        '{"code":404,"type":"error","message":"Sensor not found."}'
    )


@pytest.mark.asyncio
async def test_delete_sensor_returns_404_when_delete_finds_no_row():
    connection = MagicMock()
    pool = _pool_with_connection(connection)

    with patch.object(
        delete_sensor_ep, "set_commit", AsyncMock(return_value=None)
    ) as set_commit, patch.object(
        delete_sensor_ep, "delete_entity", AsyncMock(return_value=None)
    ) as delete_entity:
        response = await delete_sensor_ep.delete_sensor(
            sensor_id=42,
            commit_message=None,
            current_user=None,
            pool=pool,
        )

    assert response.status_code == 404
    assert response.body.decode() == (
        '{"code":404,"type":"error","message":"Sensor with id 42 not found"}'
    )
    set_commit.assert_awaited_once_with(connection, None, None, "Sensor", 42)
    delete_entity.assert_awaited_once_with(connection, "Sensor", 42)
