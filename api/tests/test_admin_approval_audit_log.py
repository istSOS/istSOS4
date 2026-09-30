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

"""Regression coverage for PATCH /Users/{id}/policy-approval, the merged
approval endpoint.

This endpoint used to have a sibling, POST /Users/{id}/activate, built
independently for OIDC signups and doing the same "promote a pending
user" job under different field names (role/dataset there vs.
assigned_role/dataset_id here) -- the two had already drifted apart:
activate logged auth_provider in its ADMIN_APPROVAL audit payload, this
one didn't, even though it already selected the row auth_provider lives
on. activate has been removed; this endpoint now covers both the local-
registration and OIDC-signup approval paths, under activate's field names
(role/dataset), which is what two of the three admin-role-grant endpoints
already used.

Covers, against the real patch_policy_approval() route function on a live
database (not just that the right SQL string was built):
  * the auth_provider audit gap is closed, for both a local- and an
    OIDC-originated pending user;
  * request.role actually overrides requested_role -- this used to be
    silently broken: the field was named assigned_role, so every caller
    that sent "role" (all of this project's own e2e scripts did) was
    silently ignored by Pydantic's default "extra fields are dropped"
    behaviour, and the endpoint fell through to requested_role every
    time, whether or not that was actually the same value;
  * a conflict (already-active user) 404s without writing a phantom
    audit row.

Same rollback-only setup/connection pattern as
test_role_reassignment.py, for the same reason: a real cleanup DELETE on
sensorthings."User" is impossible for any row that has ever been
referenced by AuditLog. Skips cleanly (not a failure) if no database is
reachable.
"""

import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import patch

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

from app import (  # noqa: E402
    ISTSOS_ADMIN,
    ISTSOS_ADMIN_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)
from fastapi import HTTPException  # noqa: E402

from app.models.approval_request import AdminApprovalRequest  # noqa: E402
from app.v1.endpoints.functions import set_role  # noqa: E402
from app.v1.endpoints.update.admin_approval import (  # noqa: E402
    patch_policy_approval,
)

_MARKER = f"_approval_audit_test_{uuid.uuid4().hex[:8]}"


async def _connect_or_skip():
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for approval-audit test: {exc}")


class _SingleConnPool:
    """Just enough of asyncpg.Pool's interface for patch_policy_approval()
    to call ``pgpool.acquire()`` and get back the one test connection, so
    the whole test still runs inside a single rollback-only transaction."""

    def __init__(self, connection):
        self._connection = connection

    def acquire(self):
        connection = self._connection

        @asynccontextmanager
        async def _acquire():
            yield connection

        return _acquire()


async def _seed_admin(connection, admin_username):
    await set_role(connection, {"role": "administrator"})
    return await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role)
        VALUES ($1, 'administrator') RETURNING id;
        """,
        admin_username,
    )


async def _seed_pending_user(
    connection, username, auth_provider, requested_role="viewer"
):
    """A 'pending' user shaped like a real registration -- auth_provider,
    requested_role, and dataset_id all set. auth_provider='local' matches
    what POST /Register writes; any other value (e.g. 'google') matches a
    JIT-provisioned OIDC signup."""
    await set_role(connection, {"role": "administrator"})
    return await connection.fetchval(
        """
        INSERT INTO sensorthings."User"
            (username, role, status, auth_provider, dataset_id,
             requested_role)
        VALUES ($1, 'pending', 'pending', $2, $3, $4)
        RETURNING id;
        """,
        username,
        auth_provider,
        f"{_MARKER}-network",
        requested_role,
    )


async def _call_policy_approval(connection, **kwargs):
    """patch_policy_approval() acquires its pool internally (write_pool =
    await get_pool_w() / get_pool()) rather than accepting it as a
    Depends() parameter. Patch both at the module level so it runs on
    this same connection/transaction -- otherwise it would open a second,
    separate connection that can't see the still-uncommitted seed rows.

    Must stay an async function that awaits patch_policy_approval() from
    inside the ``with patch(...)`` block: patch_policy_approval() is a
    coroutine function, so merely constructing (not awaiting) it here
    would do nothing, and the patches would already be reverted by the
    time a caller's own ``await`` actually ran the endpoint body.
    """
    single_pool = _SingleConnPool(connection)

    async def _get_pool():
        return single_pool

    with (
        patch("app.v1.endpoints.update.admin_approval.get_pool_w", _get_pool),
        patch("app.v1.endpoints.update.admin_approval.get_pool", _get_pool),
    ):
        return await patch_policy_approval(**kwargs)


def _admin_current_user(admin_id, username):
    return {"id": admin_id, "role": "administrator", "username": username}


def test_approval_writes_auth_provider_in_audit_event():
    """PATCH /Users/{id}/policy-approval must record auth_provider in its
    ADMIN_APPROVAL audit payload -- for a local (POST /Register) origin."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id = await _seed_admin(connection, f"{_MARKER}_admin")
                pending_id = await _seed_pending_user(
                    connection, f"{_MARKER}_local_user", "local"
                )

                response = await _call_policy_approval(
                    connection,
                    user_id=pending_id,
                    request=AdminApprovalRequest(),  # no override -> requested_role
                    current_user=_admin_current_user(admin_id, f"{_MARKER}_admin"),
                )
                assert response.status_code == 200, response.body

                audit_row = await connection.fetchrow(
                    """
                    SELECT actor_id, action_type, dataset_id, payload
                    FROM sensorthings."AuditLog"
                    WHERE action_type = 'ADMIN_APPROVAL'
                      AND (payload ->> 'approved_user_id')::bigint = $1
                    """,
                    pending_id,
                )

                assert audit_row is not None, (
                    "patch_policy_approval() returned 200 but wrote no "
                    "AuditLog row."
                )
                assert audit_row["actor_id"] == admin_id
                assert audit_row["dataset_id"] == f"{_MARKER}-network"
                payload = json.loads(audit_row["payload"])
                assert payload["granted_role"] == "viewer"
                assert payload["auth_provider"] == "local", (
                    "ADMIN_APPROVAL audit payload must include "
                    f"auth_provider -- got payload={payload!r}"
                )
                assert payload["approved_user_id"] == pending_id
            finally:
                # patch_policy_approval() ran its "async with
                # conn.transaction():" on this same connection (patched in
                # above), so it's a nested transaction (savepoint) inside
                # ours -- this single rollback undoes both.
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_approval_writes_auth_provider_for_oidc_origin():
    """The merged endpoint must cover the OIDC JIT-provisioning origin
    just as well as local registration -- this is the path that used to
    be served by the now-removed POST /Users/{id}/activate."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id = await _seed_admin(connection, f"{_MARKER}_admin2")
                pending_id = await _seed_pending_user(
                    connection, f"{_MARKER}_oidc_user", "google"
                )

                response = await _call_policy_approval(
                    connection,
                    user_id=pending_id,
                    request=AdminApprovalRequest(),
                    current_user=_admin_current_user(admin_id, f"{_MARKER}_admin2"),
                )
                assert response.status_code == 200, response.body

                audit_row = await connection.fetchrow(
                    """
                    SELECT payload FROM sensorthings."AuditLog"
                    WHERE action_type = 'ADMIN_APPROVAL'
                      AND (payload ->> 'approved_user_id')::bigint = $1
                    """,
                    pending_id,
                )
                assert audit_row is not None
                payload = json.loads(audit_row["payload"])
                assert payload["auth_provider"] == "google"
                assert payload["granted_role"] == "viewer"
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_explicit_role_overrides_requested_role():
    """request.role must actually override requested_role when supplied.

    This used to be silently broken: the field was named assigned_role,
    so sending {"role": ...} (which every one of this project's own e2e
    scripts does) was dropped by Pydantic's default "ignore extra fields"
    behaviour, and the endpoint fell through to requested_role every
    time -- the tests only ever "passed" because the requested and
    granted roles happened to be the same value in those scripts."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id = await _seed_admin(connection, f"{_MARKER}_admin3")
                pending_id = await _seed_pending_user(
                    connection,
                    f"{_MARKER}_override_user",
                    "local",
                    requested_role="viewer",
                )

                response = await _call_policy_approval(
                    connection,
                    user_id=pending_id,
                    request=AdminApprovalRequest(role="editor"),
                    current_user=_admin_current_user(admin_id, f"{_MARKER}_admin3"),
                )
                assert response.status_code == 200, response.body

                row = await connection.fetchrow(
                    'SELECT role, status FROM sensorthings."User" WHERE id = $1',
                    pending_id,
                )
                assert row["role"] == "editor", (
                    "explicit request.role='editor' must override "
                    f"requested_role='viewer' -- got role={row['role']!r}"
                )
                assert row["status"] == "active"
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_conflict_approval_does_not_log_audit_event():
    """A 404 (target not pending) must short-circuit before the
    transaction that would write the audit row -- no phantom
    ADMIN_APPROVAL entries for approvals that never actually happened."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id = await _seed_admin(connection, f"{_MARKER}_admin4")
                pending_id = await _seed_pending_user(
                    connection, f"{_MARKER}_already_active", "local"
                )
                # Already active -- the approval attempt must 404.
                await set_role(connection, {"role": "administrator"})
                await connection.execute(
                    """UPDATE sensorthings."User" SET role = 'viewer', status = 'active' WHERE id = $1;""",
                    pending_id,
                )

                with pytest.raises(HTTPException) as exc_info:
                    await _call_policy_approval(
                        connection,
                        user_id=pending_id,
                        request=AdminApprovalRequest(),
                        current_user=_admin_current_user(
                            admin_id, f"{_MARKER}_admin4"
                        ),
                    )
                assert exc_info.value.status_code == 404

                audit_row = await connection.fetchrow(
                    """
                    SELECT id FROM sensorthings."AuditLog"
                    WHERE action_type = 'ADMIN_APPROVAL'
                      AND (payload ->> 'approved_user_id')::bigint = $1
                    """,
                    pending_id,
                )
                assert audit_row is None
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())
