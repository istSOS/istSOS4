"""Tests for DB role switching in policy admin endpoints."""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("SECRET_KEY", "test_secret_key")

import app.v1.endpoints.create.policy as create_policy_endpoint  # noqa: E402
import app.v1.endpoints.update.policy as update_policy_endpoint  # noqa: E402


def _mock_pgpool(connection):
    @asynccontextmanager
    async def _acquire():
        yield connection

    class _Pool:
        def acquire(self):
            return _acquire()

    return _Pool()


def _attach_transaction_cm(connection):
    @asynccontextmanager
    async def _tx():
        yield

    connection.transaction = _tx


def test_create_policy_sets_and_resets_role_for_admin():
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchval = AsyncMock(side_effect=[0, "viewer"])
    _attach_transaction_cm(connection)

    payload = {
        "users": ["alice"],
        "name": "p1",
        "permissions": {"type": "viewer"},
    }
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        create_policy_endpoint.create_policy(
            payload=payload,
            current_user=current_user,
            pgpool=_mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]
    assert any('SET ROLE "admin_user";' in sql for sql in sql_calls)
    assert any("RESET ROLE;" in sql for sql in sql_calls)
    assert response.status_code == 201


def test_update_policy_sets_and_resets_role_for_admin():
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchrow = AsyncMock(
        return_value={"tablename": "Datastream", "cmd": "SELECT"}
    )
    _attach_transaction_cm(connection)

    payload = {"policy": "true"}
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        update_policy_endpoint.update_policy(
            policy="p1",
            payload=payload,
            current_user=current_user,
            pgpool=_mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]
    assert any('SET ROLE "admin_user";' in sql for sql in sql_calls)
    assert any("RESET ROLE;" in sql for sql in sql_calls)
    assert response.status_code == 200
