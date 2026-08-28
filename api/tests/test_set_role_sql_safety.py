import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="function")

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Patch env vars before importing app
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")

from app.v1.endpoints.functions import set_role  # noqa: E402


class DummyConnection:
    def __init__(self):
        self.execute = AsyncMock()


def test_set_role_maps_viewer_to_user_group_role():
    """Viewer app role → SET LOCAL ROLE "user"."""
    conn = DummyConnection()
    current_user = {"username": "alice", "role": "viewer"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET LOCAL ROLE "user";')


def test_set_role_maps_editor_to_user_group_role():
    """Editor app role → SET LOCAL ROLE "user" (same PG group as viewer)."""
    conn = DummyConnection()
    current_user = {"username": "bob", "role": "editor"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET LOCAL ROLE "user";')


def test_set_role_maps_sensor_to_sensor_group_role():
    """Sensor app role → SET LOCAL ROLE "sensor"."""
    conn = DummyConnection()
    current_user = {"username": "sensor01", "role": "sensor"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET LOCAL ROLE "sensor";')


def test_set_role_maps_administrator_to_administrator_group_role():
    """Administrator app role → SET LOCAL ROLE "administrator"."""
    conn = DummyConnection()
    current_user = {"username": "admin", "role": "administrator"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET LOCAL ROLE "administrator";')


def test_set_role_maps_guest_fallback():
    """Unknown role (e.g. 'guest') falls through to using the name as-is."""
    conn = DummyConnection()
    current_user = {"username": "anon", "role": "guest"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET LOCAL ROLE "guest";')


def test_set_role_rejects_invalid_identifier():
    """SQL injection attempt via role → ValueError."""
    conn = DummyConnection()
    current_user = {"username": "admin", "role": 'bad"name'}

    with pytest.raises(ValueError, match="Invalid role identifier"):
        asyncio.run(set_role(conn, current_user))

    conn.execute.assert_not_awaited()
