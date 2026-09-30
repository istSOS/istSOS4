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

"""Database logic for the PATCH /Users/{id}/role endpoint.

Design decisions
----------------
* All mutations run inside a single asyncpg transaction so a failure
  reverts atomically.

* The SELECT uses FOR UPDATE to serialize concurrent re-assignments of
  the same user row and to make the last-admin count safe under load.

* Role reassignment is a pure UPDATE on sensorthings."User".role.
  No PostgreSQL DDL (REVOKE/GRANT) is issued — users are application-layer
  entities and do not have individual PostgreSQL login roles.  The
  set_role() function in functions.py maps app roles to PG group roles
  dynamically at request time.
"""

import logging

from app import NETWORK, POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.db.audit_crud import AUDIT_ACTION_ROLE_CHANGED, log_audit_event
from app.rbac_roles import PENDING_ROLE
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


async def update_user_role(
    user_id: int,
    new_role: str | None = None,
    new_dataset: str | None = None,
    actor_id: int | None = None,
) -> None:
    """Atomically update an active user's role and/or Network scope.

    All within a single transaction:
        1. SELECT … FOR UPDATE — fetch user row; 404 if missing.
        2. Guard: pending users cannot be changed here (400).
        3. If ``new_role`` differs: last-admin lockout guard (409), then
           UPDATE role.
        4. If ``new_dataset`` is not None: validate against the Network
           table (400 if unknown), then UPDATE dataset_id. ``""`` clears
           the scope (NULL).
        5. If anything changed: write a ROLE_CHANGED AuditLog row in the
           same transaction, so a refused or failed change leaves none.

    Args:
        user_id:     Primary key of the target User row.
        new_role:    Target application role (Pydantic-validated), or None
                     to leave it unchanged.
        new_dataset: Network name to scope to, ``""`` to clear, or None to
                     leave the scope unchanged.
        actor_id:    Id of the administrator making the change (AuditLog).

    Raises:
        HTTPException 404: User not found.
        HTTPException 400: User is pending, or dataset is not a real Network.
        HTTPException 409: Would demote the last administrator.
    """
    try:
        pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
    except Exception:
        pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            row = await conn.fetchrow(
                """
                SELECT id, username, role, dataset_id
                FROM sensorthings."User"
                WHERE id = $1
                FOR UPDATE
                """,
                user_id,
            )

            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"User with id={user_id} not found.",
                )

            current_role = row["role"]
            username = row["username"]

            if current_role == PENDING_ROLE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "Cannot change a pending user here. Approve the "
                        "account first via PATCH /Users/{id}/policy-approval."
                    ),
                )

            changes = []
            final_role = current_role
            final_scope = row["dataset_id"]

            # --- role ---------------------------------------------------
            if new_role is not None and new_role != current_role:
                if current_role == "administrator":
                    admin_count = await conn.fetchval(
                        'SELECT COUNT(*) FROM sensorthings."User" '
                        "WHERE role = 'administrator'"
                    )
                    if admin_count <= 1:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail=(
                                "Cannot demote the last administrator. "
                                "Promote another user first."
                            ),
                        )
                await conn.execute(
                    'UPDATE sensorthings."User" SET role = $1 WHERE id = $2',
                    new_role,
                    user_id,
                )
                changes.append(f"role {current_role!r} -> {new_role!r}")
                final_role = new_role

            # --- network scope ----------------------------------------
            # With NETWORK=0 there are no Networks, so a requested scope is ignored.
            if new_dataset is not None and NETWORK:
                scope = new_dataset.strip() or None
                if scope is not None and NETWORK:
                    exists = await conn.fetchval(
                        'SELECT 1 FROM sensorthings."Network" WHERE name = $1',
                        scope,
                    )
                    if not exists:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=(
                                f"No Network named '{scope}'. dataset must "
                                "match an existing Network."
                            ),
                        )
                if scope != row["dataset_id"]:
                    await conn.execute(
                        'UPDATE sensorthings."User" SET dataset_id = $1 '
                        "WHERE id = $2",
                        scope,
                        user_id,
                    )
                    changes.append(
                        f"scope {row['dataset_id']!r} -> {scope!r}"
                    )
                    final_scope = scope

            if changes:
                await log_audit_event(
                    conn=conn,
                    action_type=AUDIT_ACTION_ROLE_CHANGED,
                    actor_id=actor_id,
                    dataset_id=final_scope,
                    payload={
                        "target_user_id": user_id,
                        "username": username,
                        "old_role": current_role,
                        "new_role": final_role,
                        "old_dataset_id": row["dataset_id"],
                        "new_dataset_id": final_scope,
                    },
                )

    logger.info(
        "User %r (id=%d): %s",
        username,
        user_id,
        ", ".join(changes) if changes else "no change",
    )

