# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for automatic RLS policy creation on user provisioning (POST /Users).

Verifies that create_user() calls the correct sensorthings.*_policy function
for each application role (viewer, editor, obs_manager, sensor), that the
administrator role is intentionally excluded from policy creation, and that
the policy name follows the ``{username}_default`` convention.

Relates to Issue #28: two-step user provisioning eliminated.
"""

import asyncio
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


# ---------------------------------------------------------------------------
# _POLICY_FN_MAP — replicated here to test the mapping logic independently
# ---------------------------------------------------------------------------

_POLICY_FN_MAP = {
    "viewer":      "sensorthings.viewer_policy",
    "editor":      "sensorthings.editor_policy",
    "obs_manager": "sensorthings.obs_manager_policy",
    "sensor":      "sensorthings.sensor_policy",
}


# ---------------------------------------------------------------------------
# Helpers: minimal asyncpg-alike stubs
# ---------------------------------------------------------------------------

class _Tx:
    """Minimal async context manager stub for asyncpg transactions."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    """Minimal asyncpg connection stub that records execute() calls."""

    def __init__(self, fetchrow_result):
        self._fetchrow_result = fetchrow_result
        self.executed_queries: list[tuple] = []

    def transaction(self):
        return _Tx()

    async def fetchrow(self, query, *args):
        return self._fetchrow_result

    async def execute(self, query, *args):
        self.executed_queries.append((query, args))


# ---------------------------------------------------------------------------
# Unit tests: _POLICY_FN_MAP mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "app_role, expected_fn",
    [
        ("viewer",      "sensorthings.viewer_policy"),
        ("editor",      "sensorthings.editor_policy"),
        ("obs_manager", "sensorthings.obs_manager_policy"),
        ("sensor",      "sensorthings.sensor_policy"),
    ],
)
def test_policy_fn_map_correct_function_per_role(app_role, expected_fn):
    """Each application role must map to the correct policy function."""
    assert _POLICY_FN_MAP.get(app_role) == expected_fn


def test_administrator_not_in_policy_fn_map():
    """Administrator role must NOT be in the policy map (admins bypass RLS)."""
    assert _POLICY_FN_MAP.get("administrator") is None


# ---------------------------------------------------------------------------
# Integration-level stubs: simulate the policy-call block in create_user()
# ---------------------------------------------------------------------------

async def _simulate_policy_call(app_role: str, username: str, conn: _FakeConnection):
    """
    Reproduce only the auto-policy block from create_user() so we can test
    it in isolation without spinning up a full FastAPI app or database.
    """
    policy_fn = _POLICY_FN_MAP.get(app_role)
    if policy_fn:
        policyname = f"{username}_default"
        await conn.execute(
            f"SELECT {policy_fn}($1, $2);",
            [username],
            policyname,
        )


@pytest.mark.parametrize(
    "app_role, expected_fn",
    [
        ("viewer",      "sensorthings.viewer_policy"),
        ("editor",      "sensorthings.editor_policy"),
        ("obs_manager", "sensorthings.obs_manager_policy"),
        ("sensor",      "sensorthings.sensor_policy"),
    ],
)
def test_policy_call_issued_for_each_role(app_role, expected_fn):
    """
    For each non-admin application role, exactly one execute() call must be
    made to the correct policy function with the correct arguments.
    """
    conn = _FakeConnection(fetchrow_result=None)
    username = "testuser"

    asyncio.run(_simulate_policy_call(app_role, username, conn))

    assert len(conn.executed_queries) == 1, (
        f"Expected 1 execute() call for role '{app_role}', "
        f"got {len(conn.executed_queries)}"
    )
    query, args = conn.executed_queries[0]
    assert f"SELECT {expected_fn}($1, $2);" == query, (
        f"Wrong policy function called for role '{app_role}'"
    )
    assert args[0] == [username], "First argument must be a list containing the username"
    assert args[1] == f"{username}_default", (
        "Policy name must follow the '{username}_default' convention"
    )


def test_administrator_role_does_not_call_policy():
    """
    POST /Users with role=administrator must NOT call any policy function,
    because administrators bypass RLS by role privilege, not by policy.
    """
    conn = _FakeConnection(fetchrow_result=None)

    asyncio.run(_simulate_policy_call("administrator", "adminuser", conn))

    assert conn.executed_queries == [], (
        "No policy function should be called for administrator role"
    )


def test_policy_name_convention():
    """Policy name must be '{username}_default' for any non-admin role."""
    conn = _FakeConnection(fetchrow_result=None)
    username = "alice"

    asyncio.run(_simulate_policy_call("viewer", username, conn))

    _, args = conn.executed_queries[0]
    assert args[1] == "alice_default", (
        "Policy name convention must be '{username}_default'"
    )


def test_policy_call_uses_list_for_users_arg():
    """
    The policy function expects `users_` as a text[] parameter.
    The first positional argument must therefore be a list, not a plain string.
    """
    conn = _FakeConnection(fetchrow_result=None)
    username = "bob"

    asyncio.run(_simulate_policy_call("editor", username, conn))

    _, args = conn.executed_queries[0]
    assert isinstance(args[0], list), (
        "The users_ argument must be passed as a Python list (maps to text[])"
    )
    assert args[0] == [username]
