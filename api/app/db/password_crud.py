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
* Old-password verification reuses asyncpg.connect() — the same pattern as
  oauth.get_auth_connection() — so PostgreSQL's own auth layer is the single
  source of truth.  No Python-side passlib involved.
* The actual password update uses ``ALTER USER … WITH ENCRYPTED PASSWORD``
  DDL executed through the admin write pool.  PostgreSQL's pg_crypto handles
  hashing internally (md5 / scram-sha-256 depending on pg_hba.conf).
* Both the username identifier and the new password literal are sanitised with
  pg_quote_ident / pg_quote_literal before string interpolation, since asyncpg
  does not support $N placeholders in DDL statements.
"""

import logging

import asyncpg
from app import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import pg_quote_ident, pg_quote_literal
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


async def update_local_password(
    user_id: int,
    current_password: str,
    new_password: str,
) -> None:
    """Update the PostgreSQL password for a local (non-OIDC) user.

    Execution order:
        1. Fetch the user row by ID.
        2. OIDC guard — block if auth_provider IS NOT NULL.
        3. Verify current_password via a direct asyncpg connection attempt.
        4. Execute ALTER USER … WITH ENCRYPTED PASSWORD on the write pool.

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
    # 1. Fetch user row — need username + auth_provider
    # ------------------------------------------------------------------
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, auth_provider
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

    # ------------------------------------------------------------------
    # 3. Verify current_password using PostgreSQL's own auth layer.
    #    asyncpg.connect() raises InvalidPasswordError on bad credentials.
    # ------------------------------------------------------------------
    try:
        verify_conn = await asyncpg.connect(
            user=username,
            password=current_password,
            database=POSTGRES_DB,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            timeout=5.0,
            command_timeout=10.0,
        )
        await verify_conn.close()
    except asyncpg.InvalidPasswordError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect.",
        )
    except (asyncpg.PostgresConnectionError, asyncpg.PostgresIOError) as exc:
        logger.error("DB connection error during password verification: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable.",
        )

    # ------------------------------------------------------------------
    # 4. Execute the DDL password update on the write pool.
    #    asyncpg does not support $N parameters in DDL, so we use
    #    pg_quote_ident / pg_quote_literal for safe interpolation.
    # ------------------------------------------------------------------
    try:
        from app import POSTGRES_PORT_WRITE  # import here to avoid circular

        write_pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
        async with write_pool.acquire() as conn:
            await conn.execute(
                "ALTER USER {} WITH ENCRYPTED PASSWORD {};".format(
                    pg_quote_ident(username),
                    pg_quote_literal(new_password),
                )
            )
    except asyncpg.PostgresError as exc:
        logger.error("Failed to update password for user %r: %s", username, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password.",
        )

    logger.info("Password updated for user %r (id=%d).", username, user_id)
