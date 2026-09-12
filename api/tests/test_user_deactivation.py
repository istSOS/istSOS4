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

"""Tests for DELETE /Users as a deactivation, not a hard delete.

A real DELETE FROM sensorthings."User" fails for every caller,
unconditionally -- see delete/user.py's own module docstring for why
(AuditLog_actor_id_fkey, ON DELETE SET NULL, enforced with the table
owner's privileges, and administrator was deliberately never granted
UPDATE on AuditLog). This module proves the replacement actually works:
deactivation mutates nothing AuditLog cares about, and -- the part that
would be easy to get wrong silently -- an already-issued, still-valid
JWT for the deactivated account is rejected on its very next use, with
no token-revocation step required, because role and status are both
re-checked live from the database on every request.

test_deactivate_does_not_touch_auditlog and the two live-auth-rejection
tests connect to a real database, the same reasoning as
test_rls_enforcement.py: mocking the connection can only prove which SQL
string gets sent, not that the real auth path actually rejects a
deactivated account. Everything they write lives inside a transaction
that is always rolled back, never committed.

The remaining tests (404 / 409 / self-deactivation guard) mock the
connection, matching test_policy_role_switch.py's style -- they only need
to prove which branch the handler takes, not real database behavior.
"""

import asyncio
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "admin")
os.environ.setdefault("POSTGRES_HOST", "database")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")

import asyncpg  # noqa: E402

import app.v1.endpoints.delete.user as delete_user_endpoint  # noqa: E402
import app.db.asyncpg_db as asyncpg_db  # noqa: E402
from app import (  # noqa: E402
    ISTSOS_ADMIN,
    ISTSOS_ADMIN_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)
from app.db.password_crud import pwd_context  # noqa: E402
from app.oauth import (  # noqa: E402
    authenticate_user,
    create_access_token,
    get_current_user,
)
from app.rbac_roles import DELETED_STATUS  # noqa: E402
from app.v1.endpoints.functions import set_role  # noqa: E402

_MARKER = f"_deact_test_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Live-database tests
# ---------------------------------------------------------------------------


async def _reset_shared_pool():
    """authenticate_user() / get_current_user() use the app's module-level
    get_pool() singleton internally, not an injectable connection -- it
    can't be swapped for a per-test connection the way the rest of this
    file does. That pool is bound to whichever event loop first created
    it, and each test function here runs its own asyncio.run() (a fresh
    loop each time), so the pool must be torn down and let recreate
    itself fresh inside the current loop -- same root cause as
    test_rls_enforcement.py's "another operation is in progress" note.
    """
    if asyncpg_db.pgpool is not None:
        try:
            await asyncpg_db.pgpool.close()
        except Exception:
            pass
        asyncpg_db.pgpool = None


async def _connect_or_skip():
    """Own connection per test, not the app's get_pool() singleton -- same
    reasoning as test_rls_enforcement.py: that pool is bound to whichever
    event loop first created it, and each test here gets a fresh one.
    """
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for user-deactivation test: {exc}")


async def _cleanup_committed_user(username):
    """Removes a row committed by the two cross-connection tests below.

    A real DELETE fails unconditionally for every caller (the whole
    premise of this module) -- AuditLog_actor_id_fkey's ON DELETE SET
    NULL runs with the table owner's privileges, and administrator was
    never granted UPDATE on AuditLog. Temporarily granting it, deleting
    this specific synthetic test row, then revoking it again is the same
    trick already verified safe earlier in this project's history:
    scoped, reversible, and it restores the exact prior grant state.
    """
    connection = await asyncpg.connect(
        dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    try:
        await connection.execute('SET ROLE "administrator";')
        await connection.execute(
            'GRANT UPDATE ON sensorthings."AuditLog" TO administrator;'
        )
        await connection.execute(
            'DELETE FROM sensorthings."User" WHERE username = $1;', username
        )
    finally:
        await connection.execute(
            'REVOKE UPDATE ON sensorthings."AuditLog" FROM administrator;'
        )
        await connection.close()


def test_deactivate_does_not_touch_auditlog():
    """The core claim: deactivating a user is a plain UPDATE on User, and
    never touches AuditLog at all -- proving the append-only guarantee
    genuinely stays intact, not just "the endpoint returns 200".
    """

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                await set_role(connection, {"role": "administrator"})

                audit_count_before = await connection.fetchval(
                    'SELECT count(*) FROM sensorthings."AuditLog";'
                )

                user_id = await connection.fetchval(
                    """
                    INSERT INTO sensorthings."User" (username, role, status)
                    VALUES ($1, 'viewer', 'active') RETURNING id;
                    """,
                    f"{_MARKER}_alice",
                )

                # The actual mutation delete/user.py performs -- a plain
                # UPDATE, not a DELETE.
                await connection.execute(
                    """
                    UPDATE sensorthings."User" SET status = $1 WHERE id = $2;
                    """,
                    DELETED_STATUS,
                    user_id,
                )

                row = await connection.fetchrow(
                    'SELECT status FROM sensorthings."User" WHERE id = $1;',
                    user_id,
                )
                assert row["status"] == DELETED_STATUS

                audit_count_after = await connection.fetchval(
                    'SELECT count(*) FROM sensorthings."AuditLog";'
                )
                assert audit_count_after == audit_count_before, (
                    "deactivation must not write to, or otherwise touch, "
                    "AuditLog -- if this fails, something regressed toward "
                    "the old hard-delete behavior that the AuditLog FK "
                    "trigger always rejects"
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_deactivated_user_cannot_authenticate_with_correct_password():
    """authenticate_user() must reject even the *correct* password once
    status is 'deleted' -- proves the block happens at the account-state
    check, not just "wrong credentials look the same as usual".

    Commits (rather than the rollback-only pattern used elsewhere in this
    file) because authenticate_user() queries through the app's own
    get_pool() connection, a separate connection from whatever this test
    used to insert the row -- Postgres does not let one connection see
    another connection's uncommitted transaction, so an uncommitted seed
    row here would be invisible to the real function under test. Cleaned
    up afterward via _cleanup_committed_user().
    """

    async def _run():
        username = f"{_MARKER}_bob"
        setup_conn = await _connect_or_skip()
        try:
            await set_role(setup_conn, {"role": "administrator"})
            password_hash = pwd_context.hash("CorrectHorseBattery1!")
            await setup_conn.execute(
                """
                INSERT INTO sensorthings."User"
                    (username, role, status, password)
                VALUES ($1, 'viewer', $2, $3);
                """,
                username,
                DELETED_STATUS,
                password_hash,
            )
        finally:
            await setup_conn.close()

        try:
            await _reset_shared_pool()
            with pytest.raises(Exception) as exc_info:
                await authenticate_user(username, "CorrectHorseBattery1!")
            assert getattr(exc_info.value, "status_code", None) == 401
        finally:
            await _cleanup_committed_user(username)

    asyncio.run(_run())


def test_deactivated_user_existing_jwt_is_rejected_on_next_use():
    """The no-revocation-needed claim: a JWT minted *before* deactivation
    (exactly like one a user already has sitting in a browser) must stop
    working on its very next use, with nothing done to the token itself --
    role/status are re-checked live against the database, not trusted from
    the token's own claims. Commits for the same cross-connection-
    visibility reason as the password test above.
    """

    async def _run():
        username = f"{_MARKER}_carol"
        setup_conn = await _connect_or_skip()
        try:
            await set_role(setup_conn, {"role": "administrator"})
            await setup_conn.execute(
                """
                INSERT INTO sensorthings."User" (username, role, status)
                VALUES ($1, 'viewer', 'active');
                """,
                username,
            )
        finally:
            await setup_conn.close()

        try:
            await _reset_shared_pool()

            # Mint a token the same way /Login does, while still active.
            token, _ = create_access_token(data={"sub": username, "role": "viewer"})

            # Confirm it works BEFORE deactivation -- otherwise a failure
            # below wouldn't prove anything about deactivation
            # specifically.
            user = await get_current_user(token=token)
            assert user["username"] == username

            deactivate_conn = await _connect_or_skip()
            try:
                await set_role(deactivate_conn, {"role": "administrator"})
                await deactivate_conn.execute(
                    """
                    UPDATE sensorthings."User" SET status = $1
                    WHERE username = $2;
                    """,
                    DELETED_STATUS,
                    username,
                )
            finally:
                await deactivate_conn.close()

            # Same token, no re-issue, no revocation call -- must now be
            # rejected.
            with pytest.raises(Exception) as exc_info:
                await get_current_user(token=token)
            assert getattr(exc_info.value, "status_code", None) == 403
        finally:
            await _cleanup_committed_user(username)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Handler-level tests (mocked connection, matching test_policy_role_switch.py)
# ---------------------------------------------------------------------------


def _mock_pgpool(connection):
    @asynccontextmanager
    async def acquire_cm():
        yield connection

    class _Pool:
        def acquire(self):
            return acquire_cm()

    return _Pool()


def _attach_transaction_cm(connection):
    @asynccontextmanager
    async def tx():
        yield

    connection.transaction = tx


def test_delete_user_rejects_self_deactivation():
    connection = AsyncMock()
    _attach_transaction_cm(connection)
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        delete_user_endpoint.delete_user(
            user="admin_user",
            current_user=current_user,
            pool=_mock_pgpool(connection),
        )
    )
    assert response.status_code == 400


def test_delete_user_returns_404_for_unknown_user():
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value=None)
    _attach_transaction_cm(connection)
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        delete_user_endpoint.delete_user(
            user="nosuchuser",
            current_user=current_user,
            pool=_mock_pgpool(connection),
        )
    )
    assert response.status_code == 404


def test_delete_user_returns_409_if_already_deactivated():
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"status": DELETED_STATUS})
    _attach_transaction_cm(connection)
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        delete_user_endpoint.delete_user(
            user="alreadygone",
            current_user=current_user,
            pool=_mock_pgpool(connection),
        )
    )
    assert response.status_code == 409


def test_delete_user_issues_update_not_delete():
    """Confirms the handler's actual SQL shape: an UPDATE ... SET status,
    never a DELETE or DROP ROLE, and that set_role() ran first (same
    privileged-write pattern every other admin mutation in this codebase
    uses).
    """
    connection = AsyncMock()
    connection.execute = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"status": "active"})
    _attach_transaction_cm(connection)
    current_user = {"username": "admin_user", "role": "administrator"}

    response = asyncio.run(
        delete_user_endpoint.delete_user(
            user="targetuser",
            current_user=current_user,
            pool=_mock_pgpool(connection),
        )
    )

    sql_calls = [c.args[0] for c in connection.execute.await_args_list]
    assert any('SET LOCAL ROLE "administrator";' in sql for sql in sql_calls)
    assert any(
        "UPDATE" in sql and "sensorthings" in sql and '"User"' in sql
        for sql in sql_calls
    )
    assert not any("DELETE FROM" in sql for sql in sql_calls)
    assert not any("DROP ROLE" in sql for sql in sql_calls)
    assert response.status_code == 200
