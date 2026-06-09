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

"""Database logic for the local-password update flow.

Architecture
------------
istSOS users are **application-level entities** stored in
``sensorthings."User"``.  The FastAPI backend connects to PostgreSQL via a
single master service account (``ISTSOS_ADMIN``); individual users do NOT
have their own PostgreSQL login roles.

This module therefore manages the ``password`` column in
``sensorthings."User"`` directly, using Python-side bcrypt hashing via
``passlib``.  No ``ALTER USER`` DDL or ``asyncpg.connect()``-as-user tricks
are involved.

Execution order in ``update_local_password``
--------------------------------------------
1. SELECT the user row (including the stored bcrypt hash).
2. OIDC Guard — block external identities immediately.
3. Verify the supplied ``current_password`` against the stored hash.
4. Hash the new password with bcrypt.
5. Persist via a parameterised SQL ``UPDATE``.
"""

import logging

from app.db.asyncpg_db import get_pool, get_pool_w
from fastapi import HTTPException, status
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

# Single, module-level CryptContext — bcrypt is the active scheme.
# ``deprecated="auto"`` means passlib will transparently re-hash any legacy
# hash formats on first successful verify (future-proofing).
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def update_local_password(
    user_id: int,
    current_password: str,
    new_password: str,
) -> None:
    """Update the local istSOS credential for a non-OIDC user.

    Args:
        user_id:          Primary key of the target ``sensorthings."User"`` row.
        current_password: The user's existing plaintext password (verified
                          against the stored bcrypt hash before any change is
                          made).
        new_password:     The validated new password.  Strength rules are
                          enforced by the Pydantic schema before this function
                          is called.

    Raises:
        HTTPException 404: User not found.
        HTTPException 400: User is an external OIDC identity, or has no local
                           credential set yet (``password`` column is NULL).
        HTTPException 401: ``current_password`` does not match the stored hash.
        HTTPException 500: Database error during the UPDATE.
    """
    # ------------------------------------------------------------------
    # 1. Fetch user row — need auth_provider + the stored password hash.
    # ------------------------------------------------------------------
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, auth_provider, password
            FROM sensorthings."User"
            WHERE id = $1
            """,
            user_id,
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with id={user_id} not found.",
        )

    # ------------------------------------------------------------------
    # 2. OIDC Guard — external identities have no local credential.
    # ------------------------------------------------------------------
    if row["auth_provider"] is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="External identities cannot update passwords locally.",
        )

    # ------------------------------------------------------------------
    # 3. Guard against accounts with no local credential set yet.
    #    (e.g. admin user created before migration 002 was applied, or
    #     accounts imported without a password hash.)
    # ------------------------------------------------------------------
    stored_hash = row["password"]
    if stored_hash is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No local credential is set for this account. "
                "Please contact an administrator."
            ),
        )

    # ------------------------------------------------------------------
    # 4. Verify current_password against the stored bcrypt hash.
    #    This is entirely Python-side — no extra DB round-trip.
    # ------------------------------------------------------------------
    if not pwd_context.verify(current_password, stored_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect.",
        )

    # ------------------------------------------------------------------
    # 5. Hash the new password and persist via a standard UPDATE.
    #    No DDL, no string interpolation — fully parameterised.
    # ------------------------------------------------------------------
    new_hash = pwd_context.hash(new_password)

    try:
        from app import POSTGRES_PORT_WRITE  # local import to avoid circular

        write_pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
        async with write_pool.acquire() as conn:
            await conn.execute(
                'UPDATE sensorthings."User" SET password = $1 WHERE id = $2',
                new_hash,
                user_id,
            )
    except Exception as exc:
        logger.error("Failed to update password for user id=%d: %s", user_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password.",
        )

    logger.info("Local credential updated for user id=%d.", user_id)
