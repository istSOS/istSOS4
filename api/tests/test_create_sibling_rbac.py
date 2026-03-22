"""Regression tests for RBAC hardening on sibling create endpoints."""

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

from app.v1.endpoints.create import datastream as datastream_endpoint  # noqa: E402
from app.v1.endpoints.create import observed_property as observed_property_endpoint  # noqa: E402
from app.v1.endpoints.create import feature_of_interest as foi_endpoint  # noqa: E402


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
async def test_create_datastream_denies_viewer_role():
    pool = _FakePool()

    with patch.object(datastream_endpoint, "set_role", new=AsyncMock()) as mock_set_role, patch.object(
        datastream_endpoint, "set_commit", new=AsyncMock(return_value=1)
    ), patch.object(
        datastream_endpoint,
        "insert_datastream_entity",
        new=AsyncMock(return_value=(1, "/Datastreams(1)")),
    ):
        response = await datastream_endpoint.create_datastream(
            request=_DummyRequest(),
            payload={
                "name": "datastream 1",
                "description": "d",
                "unitOfMeasurement": {"name": "Lumen", "symbol": "lm", "definition": "u"},
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "Thing": {"@iot.id": 1},
                "Sensor": {"@iot.id": 1},
                "ObservedProperty": {"@iot.id": 1},
            },
            commit_message="rbac test",
            current_user={"id": 2, "username": "viewer_user", "role": "viewer", "uri": "u"},
            pool=pool,
        )

    assert response.status_code == 401
    assert b"Insufficient privileges" in response.body
    mock_set_role.assert_not_called()


@pytest.mark.asyncio(loop_scope="function")
async def test_create_observed_property_allows_editor_role():
    pool = _FakePool()

    with patch.object(
        observed_property_endpoint, "set_role", new=AsyncMock()
    ) as mock_set_role, patch.object(
        observed_property_endpoint, "set_commit", new=AsyncMock(return_value=1)
    ), patch.object(
        observed_property_endpoint,
        "insert_observed_property_entity",
        new=AsyncMock(return_value=(1, "/ObservedProperties(1)")),
    ):
        response = await observed_property_endpoint.create_observed_property(
            request=_DummyRequest(),
            payload={
                "name": "Luminous Flux",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/LuminousFlux",
                "description": "observedProperty 1",
            },
            commit_message="rbac test",
            current_user={"id": 3, "username": "editor_user", "role": "editor", "uri": "u"},
            pool=pool,
        )

    assert response.status_code == 201
    assert response.headers.get("location") == "/ObservedProperties(1)"
    mock_set_role.assert_called_once()


@pytest.mark.asyncio(loop_scope="function")
async def test_create_feature_of_interest_denies_viewer_role():
    pool = _FakePool()

    with patch.object(foi_endpoint, "set_role", new=AsyncMock()) as mock_set_role, patch.object(
        foi_endpoint, "set_commit", new=AsyncMock(return_value=1)
    ), patch.object(
        foi_endpoint,
        "insert_feature_of_interest_entity",
        new=AsyncMock(return_value=(1, "/FeaturesOfInterest(1)")),
    ):
        response = await foi_endpoint.create_feature_of_interest(
            request=_DummyRequest(),
            payload={
                "name": "A weather station.",
                "description": "A weather station.",
                "encodingType": "application/vnd.geo+json",
                "feature": {"type": "Point", "coordinates": [-114.05, 51.05]},
            },
            commit_message="rbac test",
            current_user={"id": 2, "username": "viewer_user", "role": "viewer", "uri": "u"},
            pool=pool,
        )

    assert response.status_code == 401
    assert b"Insufficient privileges" in response.body
    mock_set_role.assert_not_called()
