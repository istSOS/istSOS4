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


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self):
        self.executed = []

    def transaction(self):
        return _Tx()

    async def execute(self, query):
        self.executed.append(query)


@pytest.mark.parametrize(
    "username, expected_query",
    [
        ("alice", 'SET ROLE "alice";'),
        ("user_1", 'SET ROLE "user_1";'),
    ],
)
def test_set_role_allows_safe_identifiers(username, expected_query):
    conn = _Conn()

    async def _run():
        await set_role(conn, {"username": username})

    asyncio.run(_run())
    assert conn.executed == [expected_query]


@pytest.mark.parametrize(
    "username",
    [
        'attacker"; RESET ROLE; --',
        "bad-user",
        "1starts_with_digit",
        "user space",
        "",
    ],
)
def test_set_role_rejects_unsafe_identifiers(username):
    conn = _Conn()

    async def _run():
        await set_role(conn, {"username": username})

    with pytest.raises(ValueError, match="Invalid role identifier"):
        asyncio.run(_run())
    assert conn.executed == []


def test_data_array_observation_uses_shared_set_role_helper():
    src = inspect.getsource(dao.data_array_observation)
    assert 'query.format(username=current_user["username"])' not in src
    assert "await set_role(conn, current_user)" in src
