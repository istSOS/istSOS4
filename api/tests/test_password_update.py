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

Coverage
--------
* Pydantic schema validation (no DB required):
    - Too short password → 422
    - No uppercase letter → 422
    - No digit → 422
    - Valid payload passes schema

* CRUD-layer unit tests (DB stubbed with mocks):
    - OIDC user blocked → HTTPException 400
    - Wrong current password → HTTPException 401
    - User not found → HTTPException 404

* Endpoint authorization (router-level, no real DB):
    - Non-owner, non-admin user → 403

All tests are synchronous (no live database required). asyncio is used only
where we need to drive async CRUD functions via asyncio.run().
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

def _make_row(auth_provider=None, username="alice", user_id=42):
    """Build a minimal asyncpg-like Record stub."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "id": user_id,
        "username": username,
        "auth_provider": auth_provider,
    }[key]
    return row


# ===========================================================================
# 1. Pydantic schema validation (no DB)
# ===========================================================================

class TestPasswordUpdateRequestSchema:

    def _import_schema(self):
        from app.models.password import PasswordUpdateRequest
        return PasswordUpdateRequest

    def test_valid_payload_passes(self):
        schema = self._import_schema()
        req = schema(current_password="OldPass1!", new_password="NewSecure1Pass!")
        assert req.new_password == "NewSecure1Pass!"

    def test_too_short_raises_422(self):
        from pydantic import ValidationError
        schema = self._import_schema()
        with pytest.raises(ValidationError) as exc_info:
            schema(current_password="old", new_password="Short1A")
        errors = exc_info.value.errors()
        assert any("12 characters" in str(e["msg"]) for e in errors)

    def test_no_uppercase_raises_422(self):
        from pydantic import ValidationError
        schema = self._import_schema()
        with pytest.raises(ValidationError) as exc_info:
            schema(current_password="old", new_password="alllowercase1pass!")
        errors = exc_info.value.errors()
        assert any("uppercase" in str(e["msg"]) for e in errors)

    def test_no_digit_raises_422(self):
        from pydantic import ValidationError
        schema = self._import_schema()
        with pytest.raises(ValidationError) as exc_info:
            schema(current_password="old", new_password="NoDigitHereAtAll!")
        errors = exc_info.value.errors()
        assert any("digit" in str(e["msg"]) for e in errors)


# ===========================================================================
# 2. CRUD-layer unit tests (DB stubbed)
# ===========================================================================

class TestUpdateLocalPasswordCRUD:
    """Tests for app.db.password_crud.update_local_password.

    The real database pool and asyncpg.connect are replaced with mocks so
    these tests run without a live Postgres instance.
    """

    def _run(self, coro):
        return asyncio.run(coro)

    # -----------------------------------------------------------------------
    # 2a. User not found → 404
    # -----------------------------------------------------------------------
    def test_user_not_found_raises_404(self):
        from fastapi import HTTPException
        from app.db import password_crud

        async def _fake_get_pool():
            pool = MagicMock()
            conn = AsyncMock()
            conn.fetchrow = AsyncMock(return_value=None)  # no row
            pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
            pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
            return pool

        with patch.object(password_crud, "get_pool", _fake_get_pool):
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
    # 2b. OIDC user (auth_provider set) → 400
    # -----------------------------------------------------------------------
    def test_oidc_user_blocked_raises_400(self):
        from fastapi import HTTPException
        from app.db import password_crud

        oidc_row = _make_row(auth_provider="google", username="alice")

        async def _fake_get_pool():
            pool = MagicMock()
            conn = AsyncMock()
            conn.fetchrow = AsyncMock(return_value=oidc_row)
            pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
            pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
            return pool

        with patch.object(password_crud, "get_pool", _fake_get_pool):
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
    # 2c. Wrong current password → 401
    # -----------------------------------------------------------------------
    def test_wrong_current_password_raises_401(self):
        import asyncpg
        from fastapi import HTTPException
        from app.db import password_crud

        local_row = _make_row(auth_provider=None, username="alice")

        async def _fake_get_pool():
            pool = MagicMock()
            conn = AsyncMock()
            conn.fetchrow = AsyncMock(return_value=local_row)
            pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
            pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
            return pool

        async def _bad_connect(**kwargs):
            raise asyncpg.InvalidPasswordError("bad password")

        with patch.object(password_crud, "get_pool", _fake_get_pool), \
             patch("asyncpg.connect", _bad_connect):
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
    # 2d. Happy path: ALTER USER DDL is executed
    # -----------------------------------------------------------------------
    def test_success_executes_alter_user(self):
        from app.db import password_crud

        local_row = _make_row(auth_provider=None, username="alice")
        executed_queries: list[str] = []

        async def _fake_get_pool():
            pool = MagicMock()
            conn = AsyncMock()
            conn.fetchrow = AsyncMock(return_value=local_row)

            async def _execute(query):
                executed_queries.append(query)

            conn.execute = _execute
            pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
            pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
            return pool

        async def _good_connect(**kwargs):
            mock_conn = AsyncMock()
            return mock_conn

        with patch.object(password_crud, "get_pool", _fake_get_pool), \
             patch("asyncpg.connect", _good_connect):
            self._run(
                password_crud.update_local_password(
                    user_id=42,
                    current_password="OldPass1!",
                    new_password="NewSecure1Pass!",
                )
            )

        assert any("ALTER USER" in q for q in executed_queries), (
            "Expected ALTER USER DDL to be executed"
        )
        assert any('"alice"' in q for q in executed_queries), (
            "Username should be quoted with pg_quote_ident"
        )


# ===========================================================================
# 3. Endpoint authorization (no real DB)
# ===========================================================================

class TestUpdatePasswordEndpointAuthorization:
    """Verify the endpoint-layer ownership/admin guard.

    We test the guard logic directly without spinning up the FastAPI app.
    """

    def test_non_owner_non_admin_is_rejected(self):
        """A regular user attempting to change someone else's password → 403."""
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
                    user_id=99,      # someone else's ID
                    payload=payload,
                    current_user=current_user,
                )
            )
        assert exc_info.value.status_code == 403
