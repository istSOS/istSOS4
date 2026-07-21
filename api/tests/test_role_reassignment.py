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

"""Tests for PATCH /Users/{id}/role.

Coverage
--------
1. Schema validation:
   - Valid roles pass Pydantic.
   - 'administrator' blocked → 422.
   - 'pending' blocked → 422.
   - Unknown role blocked → 422.

2. CRUD-layer unit tests (DB fully mocked — no live database):
   - User not found → 404.
   - Pending user → 400.
   - No-op: same role → no DDL, no UPDATE.
   - Last-admin lockout → 409.
   - viewer → obs_manager: different PG roles → REVOKE + GRANT.
   - viewer → editor: same PG role → UPDATE only, no DDL.

3. Endpoint authorization:
   - Non-admin caller → 403 (before any DB contact).
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_user_row(role="viewer", username="alice", user_id=10):
    """Build a minimal asyncpg-like Record stub."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "id": user_id,
        "username": username,
        "role": role,
    }[key]
    return row


def _make_pool(fetchrow_result, fetchval_result=None):
    """Build a pool mock that returns given fetchrow and fetchval results."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=fetchrow_result)
    conn.fetchval = AsyncMock(return_value=fetchval_result)
    conn.execute = AsyncMock()

    # Simulate transaction context manager
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=tx)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)

    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


def _run(coro):
    return asyncio.run(coro)


# ===========================================================================
# 1. Pydantic schema validation
# ===========================================================================

class TestRoleUpdateRequestSchema:

    def _schema(self):
        from app.models.role import RoleUpdateRequest
        return RoleUpdateRequest

    def test_valid_viewer_passes(self):
        s = self._schema()
        assert s(role="viewer").role == "viewer"

    def test_valid_obs_manager_passes(self):
        s = self._schema()
        assert s(role="obs_manager").role == "obs_manager"

    def test_administrator_blocked(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc:
            self._schema()(role="administrator")
        assert any("Invalid role" in str(e["msg"]) for e in exc.value.errors())

    def test_pending_blocked(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc:
            self._schema()(role="pending")
        assert any("Invalid role" in str(e["msg"]) for e in exc.value.errors())

    def test_unknown_role_blocked(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc:
            self._schema()(role="superadmin")
        assert any("Invalid role" in str(e["msg"]) for e in exc.value.errors())


# ===========================================================================
# 2. CRUD-layer unit tests (DB mocked)
# ===========================================================================

class TestUpdateUserRoleCRUD:

    # -----------------------------------------------------------------------
    # 2a. User not found → 404
    # -----------------------------------------------------------------------
    def test_user_not_found_raises_404(self):
        from fastapi import HTTPException
        from app.db import role_crud

        pool, _ = _make_pool(fetchrow_result=None)

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            with pytest.raises(HTTPException) as exc:
                _run(role_crud.update_user_role(user_id=999, new_role="editor"))
        assert exc.value.status_code == 404

    # -----------------------------------------------------------------------
    # 2b. Pending user → 400
    # -----------------------------------------------------------------------
    def test_pending_user_raises_400(self):
        from fastapi import HTTPException
        from app.db import role_crud

        pool, _ = _make_pool(fetchrow_result=_make_user_row(role="pending"))

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            with pytest.raises(HTTPException) as exc:
                _run(role_crud.update_user_role(user_id=10, new_role="editor"))
        assert exc.value.status_code == 400
        assert "pending" in exc.value.detail.lower()

    # -----------------------------------------------------------------------
    # 2c. No-op: same role → no UPDATE, no DDL
    # -----------------------------------------------------------------------
    def test_same_role_is_noop(self):
        from app.db import role_crud

        pool, conn = _make_pool(fetchrow_result=_make_user_row(role="viewer"))

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            _run(role_crud.update_user_role(user_id=10, new_role="viewer"))

        conn.execute.assert_not_called()

    # -----------------------------------------------------------------------
    # 2d. Last-admin lockout → 409
    # -----------------------------------------------------------------------
    def test_last_admin_lockout_raises_409(self):
        from fastapi import HTTPException
        from app.db import role_crud

        pool, _ = _make_pool(
            fetchrow_result=_make_user_row(role="administrator"),
            fetchval_result=1,   # only 1 admin in the system
        )

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            with pytest.raises(HTTPException) as exc:
                _run(role_crud.update_user_role(user_id=10, new_role="viewer"))
        assert exc.value.status_code == 409
        assert "last administrator" in exc.value.detail.lower()

    # -----------------------------------------------------------------------
    # 2e. Different underlying PG group → still UPDATE only, no DDL.
    #     (viewer → obs_manager: 'user' → 'sensor', but no REVOKE/GRANT)
    #     Under the app-layer credential model, set_role() handles the
    #     PG group role mapping at request time — no DDL is needed.
    # -----------------------------------------------------------------------
    def test_different_pg_group_still_no_ddl(self):
        from app.db import role_crud

        pool, conn = _make_pool(fetchrow_result=_make_user_row(role="viewer"))

        executed: list[str] = []
        async def _capture_execute(sql, *args):
            executed.append(sql)

        conn.execute = _capture_execute

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            _run(role_crud.update_user_role(user_id=10, new_role="obs_manager"))

        # UPDATE must have been called
        assert any("UPDATE" in q for q in executed), "Expected UPDATE statement"
        # No REVOKE or GRANT — we no longer issue DDL for role changes
        assert not any("REVOKE" in q for q in executed), (
            "REVOKE should NOT be issued — users are app-layer entities"
        )
        assert not any("GRANT" in q for q in executed), (
            "GRANT should NOT be issued — users are app-layer entities"
        )

    # -----------------------------------------------------------------------
    # 2f. Same underlying PG role → UPDATE only, no DDL
    #     (viewer → editor: both map to 'user')
    # -----------------------------------------------------------------------
    def test_same_pg_role_no_ddl(self):
        from app.db import role_crud

        pool, conn = _make_pool(fetchrow_result=_make_user_row(role="viewer"))

        executed: list[str] = []
        async def _capture_execute(sql, *args):
            executed.append(sql)

        conn.execute = _capture_execute

        with patch.object(role_crud, "get_pool", AsyncMock(return_value=pool)), \
             patch("app.db.role_crud.POSTGRES_PORT_WRITE", None):
            _run(role_crud.update_user_role(user_id=10, new_role="editor"))

        assert any("UPDATE" in q for q in executed), "Expected UPDATE statement"
        assert not any("REVOKE" in q for q in executed), (
            "REVOKE should NOT be issued when PG group role is unchanged"
        )
        assert not any("GRANT" in q for q in executed), (
            "GRANT should NOT be issued when PG group role is unchanged"
        )


# ===========================================================================
# 3. Endpoint authorization
# ===========================================================================

class TestPatchUserRoleEndpoint:

    def test_non_admin_caller_raises_403(self):
        """Viewer user attempting to reassign a role → 403 before any DB hit."""
        from fastapi import HTTPException
        from app.v1.endpoints.update.role import patch_user_role
        from app.models.role import RoleUpdateRequest

        caller = {"id": 1, "username": "bob", "role": "viewer"}
        payload = RoleUpdateRequest(role="editor")

        with pytest.raises(HTTPException) as exc:
            _run(
                patch_user_role(
                    user_id=5,
                    payload=payload,
                    current_user=caller,
                )
            )
        assert exc.value.status_code == 403
