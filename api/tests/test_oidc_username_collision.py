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

"""Tests for create_pending_oidc_user()'s username-collision handling.

create_pending_oidc_user() has no live HTTP caller anywhere in the
codebase yet (no OIDC callback route exists) -- these are unit tests
against the CRUD function directly, mocking the connection pool, rather
than the live-integration style used for endpoints that are actually
reachable.

Covers two distinct UniqueViolationError sources, which must be handled
differently:
  * User_username_key            -> OidcUsernameCollisionError (new,
    explicit; the underlying UniqueViolationError must NOT propagate
    unchanged, since callers shouldn't have to know the constraint name
    to distinguish this case).
  * uq_user_auth_provider_sub_id -> UniqueViolationError re-raised
    unchanged (existing, documented recovery path via
    get_user_by_provider_sub -- must not regress).
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asyncpg.exceptions import UniqueViolationError

pytestmark = pytest.mark.asyncio(loop_scope="function")

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

from app.db.oidc_user_crud import (  # noqa: E402
    OidcUsernameCollisionError,
    create_pending_oidc_user,
)


@asynccontextmanager
async def _fake_transaction():
    """conn.transaction() is a synchronous call returning something used
    as `async with ...:` -- a bare AsyncMock treats it as awaitable
    instead, which raises TypeError. Needs its own fake."""
    yield None


def _make_mock_pool(side_effect):
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(side_effect=side_effect)
    mock_conn.execute = AsyncMock(return_value=None)
    mock_conn.transaction = MagicMock(side_effect=_fake_transaction)

    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    mock_pool = MagicMock()
    mock_pool.acquire = mock_acquire
    return mock_pool


def _unique_violation(constraint_name: str) -> UniqueViolationError:
    exc = UniqueViolationError(
        f"duplicate key value violates unique constraint {constraint_name!r}"
    )
    exc.constraint_name = constraint_name
    return exc


async def test_username_collision_raises_explicit_error():
    """A User_username_key violation must raise OidcUsernameCollisionError,
    not the raw UniqueViolationError, and the message must name the
    username and describe the unresolved state."""
    mock_pool = _make_mock_pool(
        side_effect=_unique_violation("User_username_key")
    )

    with patch("app.db.oidc_user_crud.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(OidcUsernameCollisionError) as exc_info:
            await create_pending_oidc_user(
                username="jdoe",
                email="jdoe@example.com",
                auth_provider="google",
                external_sub_id="sub-123",
            )

    assert "jdoe" in str(exc_info.value)
    assert "collision" in str(exc_info.value).lower()


async def test_username_collision_error_chains_original_exception():
    """The new exception must chain the original UniqueViolationError via
    `raise ... from exc`, so the root cause is still inspectable."""
    mock_pool = _make_mock_pool(
        side_effect=_unique_violation("User_username_key")
    )

    with patch("app.db.oidc_user_crud.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(OidcUsernameCollisionError) as exc_info:
            await create_pending_oidc_user(
                username="jdoe",
                email=None,
                auth_provider="google",
                external_sub_id="sub-123",
            )

    assert isinstance(exc_info.value.__cause__, UniqueViolationError)


async def test_provider_sub_collision_still_raises_unique_violation_unchanged():
    """A uq_user_auth_provider_sub_id violation is a DIFFERENT, already-
    handled case (caller recovers via get_user_by_provider_sub) -- it
    must keep raising plain UniqueViolationError, not the new error
    type, or existing callers relying on that contract would break."""
    mock_pool = _make_mock_pool(
        side_effect=_unique_violation("uq_user_auth_provider_sub_id")
    )

    with patch("app.db.oidc_user_crud.get_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(UniqueViolationError) as exc_info:
            await create_pending_oidc_user(
                username="jdoe",
                email=None,
                auth_provider="google",
                external_sub_id="sub-123",
            )

    assert not isinstance(exc_info.value, OidcUsernameCollisionError)


async def test_successful_insert_unaffected():
    """No collision -> normal return value, untouched by this change."""
    expected_row = {
        "id": 42,
        "username": "jdoe",
        "role": "pending",
        "uri": "/Users(42)",
        "auth_provider": "google",
        "external_sub_id": "sub-123",
    }
    mock_pool = _make_mock_pool(side_effect=[expected_row])

    with patch("app.db.oidc_user_crud.get_pool", AsyncMock(return_value=mock_pool)):
        result = await create_pending_oidc_user(
            username="jdoe",
            email="jdoe@example.com",
            auth_provider="google",
            external_sub_id="sub-123",
        )

    assert result == expected_row
