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

Design decisions
----------------
* OIDC Guard runs first (before any auth attempt) so we never expose
  a timing side-channel to external-identity users.
* Modern accounts (password IS NOT NULL): old-password verification uses
  passlib/bcrypt against the stored hash in sensorthings."User".password.
* Legacy accounts (password IS NULL — pre-migration): old-password
  verification falls back to oauth.get_auth_connection() which attempts
  an asyncpg.connect() against PostgreSQL's pg_authid. On success the
  new bcrypt hash is written, completing the JIT migration.
* The actual password update is a parameterised UPDATE on
  sensorthings."User".password — no PostgreSQL DDL is issued.
"""

import logging

from app.db.asyncpg_db import get_pool, get_pool_w
from fastapi import HTTPException, status
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def update_local_password(
    user_id: int,
    current_password: str,
    new_password: str,
) -> None:
    """Update the local istSOS credential for a non-OIDC user.

    Execution order:
        1. Fetch the user row by ID (need auth_provider + password hash).
        2. OIDC guard — block if auth_provider IS NOT NULL.
        3. Verify current_password:
           a. Modern path (password IS NOT NULL): bcrypt verify.
           b. Legacy path (password IS NULL): pg_authid fallback via
              get_auth_connection().
        4. Hash new_password with bcrypt and UPDATE sensorthings."User".

    Args:
        user_id:          Primary key of the target ``sensorthings."User"`` row.
        current_password: The user's existing password (for identity verification).
        new_password:     The validated new password (strength already checked by
                          the Pydantic schema before this function is called).

    Raises:
        HTTPException 404: User not found.
        HTTPException 400: User is an external OIDC identity.
        HTTPException 401: ``current_password`` is incorrect.
        HTTPException 503: Database temporarily unavailable.
    """
    # ------------------------------------------------------------------
    # 1. Fetch user row — need username, auth_provider, and password hash
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
    # 2. OIDC Guard — external identities have no local password
    # ------------------------------------------------------------------
    if row["auth_provider"] is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="External identities cannot update passwords locally.",
        )

    username = row["username"]
    stored_hash = row["password"]

    # ------------------------------------------------------------------
    # 3. Verify current_password
    # ------------------------------------------------------------------
    if stored_hash is not None:
        # Modern path: verify against bcrypt hash in User.password
        if not pwd_context.verify(current_password, stored_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Current password is incorrect.",
            )
    else:
        # Legacy path: password IS NULL — this is a pre-migration account.
        # Fall back to pg_authid verification via get_auth_connection().
        from app.oauth import get_auth_connection

        async with get_auth_connection(username, current_password) as auth_conn:
            if auth_conn is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Current password is incorrect.",
                )

    # ------------------------------------------------------------------
    # 4. Hash the new password and write it via parameterised UPDATE.
    #    No PostgreSQL DDL — no ALTER USER, no CREATEROLE needed.
    # ------------------------------------------------------------------
    new_hash = pwd_context.hash(new_password)

    try:
        from app import POSTGRES_PORT_WRITE  # import here to avoid circular

        write_pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
        async with write_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE sensorthings."User"
                SET password = $1
                WHERE id = $2
                """,
                new_hash,
                user_id,
            )
    except Exception as exc:
        logger.error("Failed to update password for user %r: %s", username, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password.",
        )

    logger.info("Password updated for user %r (id=%d).", username, user_id)
