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

"""Regression coverage for the "OIDC activation writes no AuditLog row" gap
(see UNFINISHED_WORK.md).

create_pending_oidc_user() already logs RESTRICTED_REQUEST on signup (see
app/db/oidc_user_crud.py) -- POST /Users/{id}/activate was the one path
into an *active* role for an external identity that left no audit trail at
all, unlike its sibling update/admin_approval.py (which logs
ADMIN_APPROVAL for the local-registration path). This exercises the real
activate_user() route function against a live database so the AuditLog
INSERT is actually proven, not just that the right SQL string was built.

Same rollback-only setup/connection pattern as test_rls_enforcement.py and
test_patch_rls_silent_write_failure.py, for the same reason: a real cleanup
DELETE on sensorthings."User" is impossible for any row that has ever been
referenced by AuditLog (AuditLog_actor_id_fkey's ON DELETE SET NULL trigger
runs as the table owner, which was deliberately never granted UPDATE on
AuditLog). Skips cleanly (not a failure) if no database is reachable.
"""

import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

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
from app.v1.endpoints.create.activate_user import activate_user  # noqa: E402
from app.v1.endpoints.functions import set_role  # noqa: E402

_MARKER = f"_activate_audit_test_{uuid.uuid4().hex[:8]}"


async def _connect_or_skip():
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for activation-audit test: {exc}")


class _SingleConnPool:
    """Just enough of asyncpg.Pool's interface for activate_user() to call
    ``pgpool.acquire()`` and get back the one test connection, so the whole
    test still runs inside a single rollback-only transaction."""

    def __init__(self, connection):
        self._connection = connection

    def acquire(self):
        connection = self._connection

        @asynccontextmanager
        async def _acquire():
            yield connection

        return _acquire()


async def _seed_pending_oidc_user(connection, admin_username):
    """An administrator, plus a 'pending' user shaped like a real
    JIT-provisioned OIDC signup -- auth_provider/requested_role/dataset_id/
    odrl_policy_id all set, exactly what create_pending_oidc_user() writes."""
    await set_role(connection, {"role": "administrator"})

    admin_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role)
        VALUES ($1, 'administrator') RETURNING id;
        """,
        admin_username,
    )
    pending_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."User"
            (username, role, auth_provider, external_sub_id,
             dataset_id, odrl_policy_id, requested_role)
        VALUES ($1, 'pending', 'google', $2, $3, $4, 'viewer')
        RETURNING id;
        """,
        f"{_MARKER}_oidc_user",
        f"sub-{_MARKER}",
        f"stac://{_MARKER}-dataset",
        f"odrl:policy:{_MARKER}",
    )
    return admin_id, pending_id


def test_activation_writes_admin_approval_audit_event():
    """POST /Users/{id}/activate must leave a real, queryable AuditLog row
    -- not just return 200 -- naming the admin who acted, the user who was
    activated, and the role they were granted."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id, pending_id = await _seed_pending_oidc_user(
                    connection, f"{_MARKER}_admin"
                )

                response = await activate_user(
                    user_id=pending_id,
                    payload={},  # no override -> falls back to requested_role
                    current_user={
                        "id": admin_id,
                        "role": "administrator",
                        "username": f"{_MARKER}_admin",
                    },
                    pgpool=_SingleConnPool(connection),
                )

                assert response.status_code == 200, response.body

                audit_row = await connection.fetchrow(
                    """
                    SELECT actor_id, action_type, dataset_id, odrl_policy_id, payload
                    FROM sensorthings."AuditLog"
                    WHERE action_type = 'ADMIN_APPROVAL'
                      AND (payload ->> 'activated_user_id')::bigint = $1
                    """,
                    pending_id,
                )

                assert audit_row is not None, (
                    "activate_user() returned 200 but wrote no AuditLog row "
                    "-- an OIDC activation must be just as traceable as "
                    "PATCH /Users/{id}/policy-approval is for local users."
                )
                assert audit_row["actor_id"] == admin_id, (
                    "actor_id must record the administrator who activated "
                    f"the account, got {audit_row['actor_id']!r}"
                )
                assert audit_row["dataset_id"] == f"stac://{_MARKER}-dataset"
                assert audit_row["odrl_policy_id"] == f"odrl:policy:{_MARKER}"
                # asyncpg returns jsonb as a raw string, not an auto-parsed dict.
                payload = json.loads(audit_row["payload"])
                assert payload["granted_role"] == "viewer"
                assert payload["auth_provider"] == "google"
                assert payload["activated_user_id"] == pending_id
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_conflict_activation_does_not_log_audit_event():
    """A 409 (target not pending) must short-circuit before the transaction
    that would write the audit row -- no phantom ADMIN_APPROVAL entries for
    activations that never actually happened."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                admin_id, pending_id = await _seed_pending_oidc_user(
                    connection, f"{_MARKER}_admin2"
                )
                # Already active -- the second activate attempt must 409.
                await set_role(connection, {"role": "administrator"})
                await connection.execute(
                    """UPDATE sensorthings."User" SET role = 'viewer' WHERE id = $1;""",
                    pending_id,
                )

                response = await activate_user(
                    user_id=pending_id,
                    payload={},
                    current_user={
                        "id": admin_id,
                        "role": "administrator",
                        "username": f"{_MARKER}_admin2",
                    },
                    pgpool=_SingleConnPool(connection),
                )

                assert response.status_code == 409, response.body

                audit_row = await connection.fetchrow(
                    """
                    SELECT id FROM sensorthings."AuditLog"
                    WHERE action_type = 'ADMIN_APPROVAL'
                      AND (payload ->> 'activated_user_id')::bigint = $1
                    """,
                    pending_id,
                )
                assert audit_row is None
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())
