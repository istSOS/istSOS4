"""Tests for GET /Permissions capability contract."""

import importlib
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Force auth mode for this test module so /Permissions requires bearer auth
os.environ["AUTHORIZATION"] = "1"
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")

import app as app_package  # noqa: E402

app_package = importlib.reload(app_package)

import app.v1.endpoints.read.permissions as permissions  # noqa: E402

permissions = importlib.reload(permissions)


_NO_OVERRIDE = object()


class MockAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class MockConnection:
    def __init__(self, rows):
        self.rows = rows
        self.fetch = AsyncMock(return_value=rows)


class MockPool:
    def __init__(self, rows):
        self.connection = MockConnection(rows)

    def acquire(self):
        return MockAcquire(self.connection)


def _build_test_client(rows, current_user_override=_NO_OVERRIDE):
    test_app = FastAPI()
    test_app.include_router(permissions.v1)

    pool = MockPool(rows)

    async def override_get_pool():
        return pool

    test_app.dependency_overrides[permissions.get_pool] = override_get_pool

    if current_user_override is not _NO_OVERRIDE:

        async def override_current_user():
            return current_user_override

        test_app.dependency_overrides[
            permissions.get_current_user
        ] = override_current_user

    return TestClient(test_app), pool


def test_admin_gets_full_permissions():
    client, _ = _build_test_client(
        rows=[],
        current_user_override={"username": "admin", "role": "administrator"},
    )

    response = client.get("/Permissions")
    assert response.status_code == 200

    payload = response.json()
    assert payload["username"] == "admin"
    assert payload["role"] == "administrator"

    perms = payload["permissions"]

    for resource in [
        "users",
        "policies",
        "things",
        "sensors",
        "observations",
        "datastreams",
        "locations",
        "observed_properties",
        "features_of_interest",
        "historical_locations",
    ]:
        assert perms[resource]["read"] is True
        assert perms[resource]["create"] is True
        assert perms[resource]["update"] is True
        assert perms[resource]["delete"] is True

    assert perms["audit_log"]["read"] is True
    assert perms["perm_matrix"]["read"] is True


def test_non_admin_policy_mapping_and_all_command():
    rows = [
        {"tablename": "Thing", "cmd": "SELECT"},
        {"tablename": "Thing", "cmd": "INSERT"},
        {"tablename": "Observation", "cmd": "ALL"},
        {"tablename": "FeatureOfInterest", "cmd": "UPDATE"},
    ]

    client, pool = _build_test_client(
        rows=rows,
        current_user_override={"username": "alice", "role": "editor"},
    )

    response = client.get("/Permissions")
    assert response.status_code == 200

    pool.connection.fetch.assert_awaited_once()

    perms = response.json()["permissions"]

    assert perms["things"] == {
        "read": True,
        "create": True,
        "update": False,
        "delete": False,
    }

    assert perms["observations"] == {
        "read": True,
        "create": True,
        "update": True,
        "delete": True,
    }

    assert perms["features_of_interest"] == {
        "read": False,
        "create": False,
        "update": True,
        "delete": False,
    }

    assert perms["users"] == {
        "read": False,
        "create": False,
        "update": False,
        "delete": False,
    }


def test_non_admin_with_no_policies_returns_safe_defaults():
    client, _ = _build_test_client(
        rows=[],
        current_user_override={"username": "viewer1", "role": "viewer"},
    )

    response = client.get("/Permissions")
    assert response.status_code == 200

    perms = response.json()["permissions"]

    assert perms["things"] == {
        "read": False,
        "create": False,
        "update": False,
        "delete": False,
    }
    assert perms["audit_log"] == {"read": False}
    assert perms["perm_matrix"] == {"read": False}


def test_permissions_endpoint_requires_auth_when_not_overridden():
    client, _ = _build_test_client(rows=[])

    response = client.get("/Permissions")

    assert response.status_code == 401


def test_malformed_current_user_returns_401():
    client, _ = _build_test_client(rows=[], current_user_override={})

    response = client.get("/Permissions")

    assert response.status_code == 401
    assert response.json() == {"message": "Could not validate credentials"}


def test_role_membership_is_used_for_policy_lookup():
    rows = [{"tablename": "Datastream", "cmd": "SELECT"}]

    client, pool = _build_test_client(
        rows=rows,
        current_user_override={"username": "alice", "role": "editor"},
    )

    response = client.get("/Permissions")
    assert response.status_code == 200

    pool.connection.fetch.assert_awaited_once()
    await_args = pool.connection.fetch.await_args
    assert await_args is not None
    fetch_args = await_args.args
    assert fetch_args[1] == "alice"
    assert fetch_args[2] == "editor"

    perms = response.json()["permissions"]
    assert perms["datastreams"] == {
        "read": True,
        "create": False,
        "update": False,
        "delete": False,
    }


def test_response_schema_forbids_extra_fields():
    payload = {
        "username": "alice",
        "role": "editor",
        "permissions": permissions.PermissionsPayload().model_dump(),
        "unknown_key": True,
    }

    with pytest.raises(ValidationError):
        permissions.PermissionsResponse.model_validate(payload)
