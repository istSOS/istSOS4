"""Regression tests for custom policy expression handling."""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest


API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("SECRET_KEY", "test_secret_key")

from app.utils.policy_expression import render_policy_expression  # noqa: E402
import app.v1.endpoints.create.policy as create_policy_endpoint  # noqa: E402
import app.v1.endpoints.update.policy as update_policy_endpoint  # noqa: E402


def test_render_policy_expression_normalizes_simple_condition():
    assert (
        render_policy_expression("network = 'IDROLOGIA' and public = true")
        == '("network" = \'IDROLOGIA\' AND "public" = TRUE)'
    )


def test_render_policy_expression_keeps_quoted_identifier_case():
    assert (
        render_policy_expression('"unitOfMeasurement" is not null')
        == '"unitOfMeasurement" IS NOT NULL'
    )


@pytest.mark.parametrize(
    "expression",
    [
        "true) WITH CHECK (false",
        "network = 'x'; DROP TABLE sensorthings.\"User\"",
        "exists (select 1)",
        "network = 'unterminated",
    ],
)
def test_render_policy_expression_rejects_sql_structure(expression):
    with pytest.raises(ValueError):
        render_policy_expression(expression)


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


def test_update_policy_uses_normalized_policy_condition():
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchrow = AsyncMock(
        return_value={"tablename": "Datastream", "cmd": "SELECT"}
    )
    _attach_transaction_cm(connection)

    response = asyncio.run(
        update_policy_endpoint.update_policy(
            policy="p1",
            payload={"policy": "network = 'IDROLOGIA'"},
            current_user={"username": "admin_user", "role": "administrator"},
            pgpool=_mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]

    assert response.status_code == 200
    assert (
        'ALTER POLICY "p1" ON sensorthings."Datastream" '
        "USING (\"network\" = 'IDROLOGIA')"
    ) in sql_calls


def test_update_policy_rejects_unrenderable_policy_condition():
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchrow = AsyncMock(
        return_value={"tablename": "Datastream", "cmd": "SELECT"}
    )
    _attach_transaction_cm(connection)

    response = asyncio.run(
        update_policy_endpoint.update_policy(
            policy="p1",
            payload={"policy": "true) WITH CHECK (false"},
            current_user={"username": "admin_user", "role": "administrator"},
            pgpool=_mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]

    assert response.status_code == 400
    assert not any(sql.startswith("ALTER POLICY") for sql in sql_calls)


def test_create_policies_rejects_unrenderable_policy_condition():
    connection = AsyncMock()
    connection.execute = AsyncMock()

    with pytest.raises(ValueError):
        asyncio.run(
            create_policy_endpoint.create_policies(
                connection=connection,
                users=["alice"],
                policies={
                    "datastream": {
                        "select": "true) TO PUBLIC USING (true",
                    }
                },
                name="rbac_test",
            )
        )

    connection.execute.assert_not_awaited()
