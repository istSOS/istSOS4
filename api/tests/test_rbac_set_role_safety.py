import asyncio
import inspect
import os
import sys
from pathlib import Path

import pytest

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")

import app.v1.endpoints.create.data_array_observation as dao
from app.v1.endpoints.functions import set_role


class _Conn:
    def __init__(self):
        self.executed = []

    async def execute(self, query):
        self.executed.append(query)


@pytest.mark.parametrize(
    "app_role, expected_query",
    [
        ("viewer", 'SET LOCAL ROLE "user";'),
        ("editor", 'SET LOCAL ROLE "user";'),
        ("obs_manager", 'SET LOCAL ROLE "sensor";'),
        ("sensor", 'SET LOCAL ROLE "sensor";'),
        ("administrator", 'SET LOCAL ROLE "administrator";'),
    ],
)
def test_set_role_maps_app_role_to_pg_group_role(app_role, expected_query):
    """set_role() maps each application role to the correct PG group role."""
    conn = _Conn()

    async def _run():
        await set_role(conn, {"username": "test_user", "role": app_role})

    asyncio.run(_run())
    assert conn.executed == [expected_query]


@pytest.mark.parametrize(
    "role",
    [
        'attacker"; RESET ROLE; --',
        "bad-user",
        "1starts_with_digit",
        "user space",
        "",
    ],
)
def test_set_role_rejects_unsafe_identifiers(role):
    """Roles that fail identifier validation raise ValueError."""
    conn = _Conn()

    async def _run():
        await set_role(conn, {"username": "test_user", "role": role})

    with pytest.raises(ValueError, match="Invalid role identifier"):
        asyncio.run(_run())
    assert conn.executed == []


def test_data_array_observation_uses_shared_set_role_helper():
    src = inspect.getsource(dao.data_array_observation)
    assert 'query.format(username=current_user["username"])' not in src
    assert "await set_role(conn, current_user)" in src
