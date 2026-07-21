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

Coverage
--------
Modern accounts (password IS NOT NULL):
  * User not found → None
  * OIDC user (auth_provider set) → None
  * Wrong password → None
  * Correct password → {"sub", "role"}

Legacy accounts (password IS NULL):
  * Wrong legacy password (fallback returns None) → None
  * Correct legacy password → {"sub", "role"} AND bcrypt hash written to DB
    (JIT migration)
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
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


def _make_pool(row, execute_capture=None):
    """Return a fake asyncpg pool.

    If ``execute_capture`` is a list, any ``conn.execute()`` call appends
    ``(query, args)`` to it.
    """
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=row)

    if execute_capture is not None:
        async def _capture(query, *args):
            execute_capture.append((query, list(args)))
        conn.execute = _capture
    else:
        conn.execute = AsyncMock()

    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


def _make_row(username="alice", role="viewer", password=_FAKE_HASH, auth_provider=None):
    """Build a minimal asyncpg-like Record stub."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "id": 42,
        "username": username,
        "role": role,
        "password": password,
        "auth_provider": auth_provider,
    }[key]
    return row


def _legacy_auth_ctx(success: bool):
    """Return a fake get_auth_connection context manager.

    Yields a mock connection object when ``success=True``, or ``None`` when
    ``success=False`` (simulating a wrong legacy password).
    """
    @asynccontextmanager
    async def _ctx(username, password):
        yield MagicMock() if success else None
    return _ctx


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
    # OIDC user (auth_provider IS NOT NULL) → None
    # -----------------------------------------------------------------------
    def test_oidc_user_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row(auth_provider="google", password=None))
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)):
            result = self._run(oauth.authenticate_user("alice", "SomePass1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # Modern account — wrong password → None
    # -----------------------------------------------------------------------
    def test_wrong_password_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row())
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth.pwd_context, "verify", return_value=False):
            result = self._run(oauth.authenticate_user("alice", "WrongPass1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # Modern account — correct password → user dict
    # -----------------------------------------------------------------------
    def test_correct_password_returns_user_dict(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row(username="alice", role="editor"))
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth.pwd_context, "verify", return_value=True):
            result = self._run(oauth.authenticate_user("alice", "CorrectPass1!"))
        assert result == {"sub": "alice", "role": "editor"}

    # -----------------------------------------------------------------------
    # Legacy account — wrong legacy password → None
    # -----------------------------------------------------------------------
    def test_legacy_wrong_password_returns_none(self):
        import app.oauth as oauth

        pool = _make_pool(row=_make_row(password=None))
        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth, "get_auth_connection", _legacy_auth_ctx(success=False)):
            result = self._run(oauth.authenticate_user("alice", "WrongLegacy1!"))
        assert result is None

    # -----------------------------------------------------------------------
    # Legacy account — correct legacy password → user dict + JIT hash written
    # -----------------------------------------------------------------------
    def test_legacy_correct_password_upgrades_hash(self):
        import app.oauth as oauth

        executed: list = []
        pool = _make_pool(row=_make_row(password=None), execute_capture=executed)
        fake_hash = "$2b$12$jit_upgraded_hash_for_testing."

        with patch.object(oauth, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(oauth, "get_auth_connection", _legacy_auth_ctx(success=True)), \
             patch.object(oauth.pwd_context, "hash", return_value=fake_hash):
            result = self._run(oauth.authenticate_user("alice", "OldLegacyPass1!"))

        # Should return the token payload
        assert result == {"sub": "alice", "role": "viewer"}

        # Should have written the new bcrypt hash to the DB
        assert len(executed) == 1, "Expected exactly one UPDATE statement"
        query, args = executed[0]
        assert "UPDATE" in query.upper()
        assert fake_hash in args
        assert 42 in args  # user_id
