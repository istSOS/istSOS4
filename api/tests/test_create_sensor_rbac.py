"""Regression tests for RBAC on POST /Sensors."""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Patch env vars before importing app modules
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")

from app.v1.endpoints.create import sensor as sensor_endpoint  # noqa: E402


class _DummyRequest:
    def __init__(self):
        self.headers = {"content-type": "application/json"}


class _FakeConnection:
    @asynccontextmanager
    async def transaction(self):
        yield


class _FakePool:
    def __init__(self):
        self.connection = _FakeConnection()

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


@pytest.mark.asyncio(loop_scope="function")
async def test_create_sensor_denies_viewer_role():
    """Viewer must not be able to create sensors."""
    pool = _FakePool()

    with patch.object(sensor_endpoint, "set_role", new=AsyncMock()) as mock_set_role, patch.object(
        sensor_endpoint, "set_commit", new=AsyncMock(return_value=1)
    ), patch.object(
        sensor_endpoint,
        "insert_sensor_entity",
        new=AsyncMock(return_value=(1, "/Sensors(1)")),
    ):
        response = await sensor_endpoint.create_sensor(
            request=_DummyRequest(),
            payload={
                "name": "sensor name 1",
                "encodingType": "application/pdf",
                "metadata": "Light flux sensor",
            },
            commit_message="rbac test",
            current_user={"id": 2, "username": "viewer_user", "role": "viewer", "uri": "u"},
            pool=pool,
        )

    assert response.status_code == 401
    assert b"Insufficient privileges" in response.body
    mock_set_role.assert_not_called()


@pytest.mark.asyncio(loop_scope="function")
async def test_create_sensor_allows_editor_role():
    """Editor should still be able to create sensors."""
    pool = _FakePool()

    with patch.object(sensor_endpoint, "set_role", new=AsyncMock()) as mock_set_role, patch.object(
        sensor_endpoint, "set_commit", new=AsyncMock(return_value=1)
    ), patch.object(
        sensor_endpoint,
        "insert_sensor_entity",
        new=AsyncMock(return_value=(1, "/Sensors(1)")),
    ):
        response = await sensor_endpoint.create_sensor(
            request=_DummyRequest(),
            payload={
                "name": "sensor name 1",
                "encodingType": "application/pdf",
                "metadata": "Light flux sensor",
            },
            commit_message="rbac test",
            current_user={"id": 3, "username": "editor_user", "role": "editor", "uri": "u"},
            pool=pool,
        )

    assert response.status_code == 201
    assert response.headers.get("location") == "/Sensors(1)"
    mock_set_role.assert_called_once()
