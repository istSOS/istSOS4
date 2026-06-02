"""Regression tests for custom policy role target generation."""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Provide minimal env vars before importing app modules.
os.environ.setdefault("SECRET_KEY", "test_secret_key")

import app.v1.endpoints.create.policy as create_policy_endpoint  # noqa: E402


def _run_create_policies(users):
    """Execute create_policies and return emitted SQL statements."""
    connection = AsyncMock()
    connection.execute = AsyncMock()

    policies = {"datastream": {"select": "true"}}

    asyncio.run(
        create_policy_endpoint.create_policies(
            connection=connection,
            users=users,
            policies=policies,
            name="rbac_test",
        )
    )

    return [call.args[0] for call in connection.execute.await_args_list]


def test_create_policies_multi_user_targets_are_separate_roles():
    """Multiple users must be rendered as separate quoted role identifiers."""
    statements = _run_create_policies(["alice", "bob"])

    assert len(statements) == 1
    sql = statements[0]
    assert 'TO "alice", "bob"' in sql
    assert 'TO "alice, bob"' not in sql


def test_create_policies_single_user_target_still_valid():
    """Single-user policy generation should remain unchanged and valid."""
    statements = _run_create_policies(["alice"])

    assert len(statements) == 1
    sql = statements[0]
    assert 'TO "alice"' in sql
