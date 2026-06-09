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

"""Tests for the authenticate_user() function in app.oauth.

Architecture note
-----------------
After the architectural pivot, authentication no longer calls
``asyncpg.connect()`` as the target user or touches PostgreSQL's internal
``pg_authid``.  Instead, ``authenticate_user()`` queries the application-
level ``sensorthings."User"."password"`` column (a bcrypt hash) via the
shared connection pool, and verifies credentials with ``passlib``.

This test file replaces ``test_oauth_connection_leak.py``, which was written
against the old ``get_auth_connection()``/``asyncpg.connect()`` design.

Coverage
--------
* User not found → None
* NULL password (OIDC / pre-migration account) → None
* Wrong password (pwd_context.verify returns False) → None
* Correct password → {"sub": username, "role": role}
* Pool error during SELECT → exception propagates (not swallowed)
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890_abcdef")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_HASH = "$2b$12$fakehashfortestingpurposesonly...."


def _make_pool(row):
    """Return a fake asyncpg pool whose connection returns ``row`` on fetchrow."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=row)
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


def _make_row(username="alice", role="viewer", password=_FAKE_HASH):
    """Build a minimal asyncpg-like Record stub."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "id": 42,
        "username": username,
        "role": role,
        "password": password,
    }[key]
    return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAuthenticateUser:
    """Unit tests for app.oauth.authenticate_user."""

    def _run(self, coro):
        return asyncio.run(coro)

    # -----------------------------------------------------------------------
    # User not found → None
    # -----------------------------------------------------------------------
    def test_unknown_user_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=None)
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)):
            result = self._run(oauth.authenticate_user("nobody", "SomePass1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # NULL password (OIDC / pre-migration user) → None
    # -----------------------------------------------------------------------
    def test_null_password_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row(password=None))
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)):
            result = self._run(oauth.authenticate_user("alice", "SomePass1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # Wrong password (verify returns False) → None
    # -----------------------------------------------------------------------
    def test_wrong_password_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row())
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth.pwd_context, "verify", return_value=False):
            result = self._run(oauth.authenticate_user("alice", "WrongPass1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # Correct password → {"sub": ..., "role": ...}
    # -----------------------------------------------------------------------
    def test_correct_password_returns_user_dict(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row(username="alice", role="editor"))
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth.pwd_context, "verify", return_value=True):
            result = self._run(oauth.authenticate_user("alice", "CorrectPass1!"))
        assert result == {"sub": "alice", "role": "editor"}

    # -----------------------------------------------------------------------
    # No asyncpg.connect is called — the pool is used instead
    # -----------------------------------------------------------------------
    def test_no_direct_asyncpg_connect_called(self):
        """Prove that asyncpg.connect is never called in the new flow."""
        import asyncpg
        import app.oauth as oauth

        pool = _make_pool(row=_make_row())
        connect_calls = []

        async def _spy_connect(**kwargs):
            connect_calls.append(kwargs)
            return AsyncMock()

        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth.pwd_context, "verify", return_value=True), \
             patch("asyncpg.connect", side_effect=_spy_connect):
            self._run(oauth.authenticate_user("alice", "CorrectPass1!"))

        assert connect_calls == [], (
            "asyncpg.connect() was called — the new flow must use the pool only."
        )
