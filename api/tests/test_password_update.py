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

"""Tests for PATCH /Users/{id}/password.

Architecture note
-----------------
istSOS users are application-level entities; they do NOT have individual
PostgreSQL login roles.  The FastAPI backend connects via a single master
service account.  Passwords are stored as bcrypt hashes in the
``sensorthings."User"."password"`` column and managed entirely on the
Python side using ``passlib``.

Coverage
--------
* Pydantic schema validation (no DB required):
    - Too short password → 422
    - No uppercase letter → 422
    - No digit → 422
    - Valid payload passes schema

* CRUD-layer unit tests (DB stubbed, passlib mocked):
    - User not found → HTTPException 404
    - OIDC user (auth_provider set) → HTTPException 400
    - Account with no local credential (password IS NULL) → HTTPException 400
    - Wrong current password (pwd_context.verify returns False) → HTTPException 401
    - Success: UPDATE executed with new bcrypt hash → no exception

* Endpoint authorization (router-level, no real DB):
    - Non-owner, non-admin user → 403

All tests are synchronous (no live database required). ``asyncio.run()``
drives async functions under test.
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap — allow imports without installing the package
# ---------------------------------------------------------------------------
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Minimal env so app/__init__.py does not raise on import
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890_abcdef")
os.environ.setdefault("ALGORITHM", "HS256")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_HASH = "$2b$12$fakehashfortestingpurposesonly...."


def _make_row(
    auth_provider=None,
    username="alice",
    user_id=42,
    password=_FAKE_HASH,
):
    """Build a minimal asyncpg-like Record stub.

    The ``password`` field defaults to a fake bcrypt-shaped hash so that
    tests don't accidentally hit the NULL-credential guard unless they
    explicitly pass ``password=None``.
    """
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "id": user_id,
        "username": username,
        "auth_provider": auth_provider,
        "password": password,
    }[key]
    return row


def _make_pool(row=None, execute_capture=None):
    """Return a fake asyncpg pool whose connection returns ``row`` on fetchrow.

    If ``execute_capture`` is a list, tuples of (query, args) are appended
    to it every time ``conn.execute()`` is called.
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


# ===========================================================================
# 1. Pydantic schema validation (no DB)
# ===========================================================================

class TestPasswordUpdateRequestSchema:

    def _schema(self):
        from app.models.password import PasswordUpdateRequest
        return PasswordUpdateRequest

    def test_valid_payload_passes(self):
        req = self._schema()(current_password="OldPass1!", new_password="NewSecure1Pass!")
        assert req.new_password == "NewSecure1Pass!"

    def test_too_short_raises_422(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            self._schema()(current_password="old", new_password="Short1A")
        assert any("12 characters" in str(e["msg"]) for e in exc_info.value.errors())

    def test_no_uppercase_raises_422(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            self._schema()(current_password="old", new_password="alllowercase1pass!")
        assert any("uppercase" in str(e["msg"]) for e in exc_info.value.errors())

    def test_no_digit_raises_422(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            self._schema()(current_password="old", new_password="NoDigitHereAtAll!")
        assert any("digit" in str(e["msg"]) for e in exc_info.value.errors())


# ===========================================================================
# 2. CRUD-layer unit tests (DB stubbed, passlib mocked)
# ===========================================================================

class TestUpdateLocalPasswordCRUD:
    """Tests for ``app.db.password_crud.update_local_password``.

    The asyncpg pool and passlib's CryptContext are replaced with mocks so
    these tests run without a live Postgres instance or real bcrypt work.
    """

    def _run(self, coro):
        return asyncio.run(coro)

    # -----------------------------------------------------------------------
    # 2a. User not found → 404
    # -----------------------------------------------------------------------
    def test_user_not_found_raises_404(self):
        from fastapi import HTTPException
        from app.db import password_crud

        pool = _make_pool(row=None)
        with patch.object(password_crud, "get_pool", AsyncMock(return_value=pool)):
            with pytest.raises(HTTPException) as exc_info:
                self._run(
                    password_crud.update_local_password(
                        user_id=999,
                        current_password="OldPass1!",
                        new_password="NewSecure1Pass!",
                    )
                )
        assert exc_info.value.status_code == 404

    # -----------------------------------------------------------------------
    # 2b. OIDC user (auth_provider IS NOT NULL) → 400
    # -----------------------------------------------------------------------
    def test_oidc_user_blocked_raises_400(self):
        from fastapi import HTTPException
        from app.db import password_crud

        pool = _make_pool(row=_make_row(auth_provider="google"))
        with patch.object(password_crud, "get_pool", AsyncMock(return_value=pool)):
            with pytest.raises(HTTPException) as exc_info:
                self._run(
                    password_crud.update_local_password(
                        user_id=42,
                        current_password="OldPass1!",
                        new_password="NewSecure1Pass!",
                    )
                )
        assert exc_info.value.status_code == 400
        assert "External identities" in exc_info.value.detail

    # -----------------------------------------------------------------------
    # 2c. No local credential (password IS NULL) → 400
    # -----------------------------------------------------------------------
    def test_null_password_raises_400(self):
        from fastapi import HTTPException
        from app.db import password_crud

        pool = _make_pool(row=_make_row(auth_provider=None, password=None))
        with patch.object(password_crud, "get_pool", AsyncMock(return_value=pool)):
            with pytest.raises(HTTPException) as exc_info:
                self._run(
                    password_crud.update_local_password(
                        user_id=42,
                        current_password="OldPass1!",
                        new_password="NewSecure1Pass!",
                    )
                )
        assert exc_info.value.status_code == 400
        assert "No local credential" in exc_info.value.detail

    # -----------------------------------------------------------------------
    # 2d. Wrong current password (verify returns False) → 401
    # -----------------------------------------------------------------------
    def test_wrong_current_password_raises_401(self):
        from fastapi import HTTPException
        from app.db import password_crud

        pool = _make_pool(row=_make_row(auth_provider=None))
        with patch.object(password_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch.object(password_crud.pwd_context, "verify", return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                self._run(
                    password_crud.update_local_password(
                        user_id=42,
                        current_password="WrongPass1!",
                        new_password="NewSecure1Pass!",
                    )
                )
        assert exc_info.value.status_code == 401
        assert "incorrect" in exc_info.value.detail.lower()

    # -----------------------------------------------------------------------
    # 2e. Happy path: UPDATE issued with the new hash → no exception
    # -----------------------------------------------------------------------
    def test_success_executes_update_with_hash(self):
        from app.db import password_crud

        executed: list = []
        read_pool  = _make_pool(row=_make_row(auth_provider=None))
        write_pool = _make_pool(row=_make_row(auth_provider=None), execute_capture=executed)
        fake_hash  = "$2b$12$newhashedvalue_for_testing_only."

        with patch.object(password_crud, "get_pool",   AsyncMock(return_value=read_pool)), \
             patch.object(password_crud, "get_pool_w", AsyncMock(return_value=write_pool)), \
             patch.object(password_crud.pwd_context, "verify", return_value=True), \
             patch.object(password_crud.pwd_context, "hash",   return_value=fake_hash), \
             patch("app.POSTGRES_PORT_WRITE", "5433"):
            self._run(
                password_crud.update_local_password(
                    user_id=42,
                    current_password="OldPass1!",
                    new_password="NewSecure1Pass!",
                )
            )

        assert len(executed) == 1, "Expected exactly one SQL statement"
        query, args = executed[0]
        assert "UPDATE" in query.upper(), "Expected UPDATE, not DDL"
        assert "ALTER"  not in query.upper(), "No DDL ALTER should appear"
        assert fake_hash in args, "New bcrypt hash must be a parameterised arg"
        assert 42 in args, "user_id must be a parameterised arg"


# ===========================================================================
# 3. Endpoint authorization (no real DB)
# ===========================================================================

class TestUpdatePasswordEndpointAuthorization:
    """Verify the endpoint-layer ownership/admin guard without spinning up FastAPI."""

    def test_non_owner_non_admin_is_rejected(self):
        """A viewer trying to change someone else's password → 403."""
        from fastapi import HTTPException
        from app.v1.endpoints.update.password import update_password
        from app.models.password import PasswordUpdateRequest

        current_user = {"id": 1, "username": "bob", "role": "viewer"}
        payload = PasswordUpdateRequest(
            current_password="OldPass1!",
            new_password="NewSecure1Pass!",
        )

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                update_password(
                    user_id=99,          # someone else's ID
                    payload=payload,
                    current_user=current_user,
                )
            )
        assert exc_info.value.status_code == 403
