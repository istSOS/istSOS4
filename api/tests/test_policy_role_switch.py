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


def mock_pgpool(connection):
    @asynccontextmanager
    async def acquire_cm():
        yield connection

    class _Pool:
        def acquire(self):
            return acquire_cm()

    return _Pool()


def attach_transaction_cm(connection):
    @asynccontextmanager
    async def tx():
        yield

    connection.transaction = tx


def test_create_policy_rejects_role_types_covered_by_static_policies():
    """viewer/editor/obs_manager/sensor/qc get RLS access automatically from
    006_session_scoped_rls_policies.sql's static policies the moment their
    role is set -- POST /Policies has nothing to create for them, and
    returns 400 rather than a 200 the caller would have to read the body
    of to learn nothing happened. It still switches to administrator first
    (matching every other admin-only mutation), but issues no DDL.
    """
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchval = AsyncMock(return_value=0)
    attach_transaction_cm(connection)

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
            pgpool=mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]
    assert any('SET LOCAL ROLE "administrator";' in sql for sql in sql_calls)
    assert not any("RESET ROLE" in sql for sql in sql_calls)
    assert not any("_policy(" in sql for sql in sql_calls)
    assert not any("CREATE POLICY" in sql for sql in sql_calls)
    assert response.status_code == 400
    assert "static RLS policy" in response.body.decode()


# PATCH /Policies was removed: in-place policy editing depended on the
# per-PG-role model. Policy changes are now DELETE + POST.


def test_custom_policy_only_applies_while_user_is_custom():
    """Every generated custom policy must check the caller's live role, so a
    user later moved to viewer/editor does not keep these grants on top of
    their new role's (they become dormant, not deleted)."""
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetch = AsyncMock(
        return_value=[{"username": "alice", "id": 7, "role": "custom"}]
    )

    asyncio.run(
        create_policy_endpoint.create_policies(
            connection,
            ["alice"],
            {"datastream": {"select": "id IN (1, 2)", "update": "id = 2"}},
            "p1",
        )
    )

    created = [
        c.args[0]
        for c in connection.execute.await_args_list
        if "CREATE POLICY" in c.args[0]
    ]
    assert len(created) == 2
    for sql in created:
        assert "sensorthings.current_app_user_role() = 'custom'" in sql
        assert "sensorthings.current_app_user_id() = ANY (ARRAY[7]::bigint[])" in sql
