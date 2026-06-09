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

"""Authentication helpers for the istSOS FastAPI application.

Architecture
------------
istSOS users are **application-level entities** stored in
``sensorthings."User"``.  The backend connects to PostgreSQL via a single
master service account (``ISTSOS_ADMIN``).  Individual users do NOT have
their own PostgreSQL login roles.

Local credential verification uses ``passlib`` (bcrypt) against the
``password`` column in ``sensorthings."User"``.  For legacy accounts created
before migration 002 (where ``password IS NULL``), ``get_auth_connection()``
is used as a transparent fallback that simultaneously migrates the credential
to the application layer — so the next login uses bcrypt directly.

OIDC users (``auth_provider IS NOT NULL``) are authenticated exclusively
through their external identity provider and are blocked from the
``/Login`` endpoint.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import jwt
from app import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    ALGORITHM,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    REDIS,
    SECRET_KEY,
)
from app.db.asyncpg_db import get_pool
from app.db.password_crud import pwd_context
from app.db.redis_db import redis
from app.rbac_roles import PENDING_ROLE
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="Login")


async def get_user_from_db(username: str):
    """Fetch the user dict from ``sensorthings."User"`` by username.

    Returns a dict with ``{id, username, role, uri}`` or ``None`` if the
    username does not exist.
    """
    pool = await get_pool()
    async with pool.acquire() as connection:
        query = """
            SELECT id, username, role, uri
            FROM sensorthings."User"
            WHERE username = $1
        """
        user_record = await connection.fetchrow(query, username)
        if user_record is not None:
            return {
                "id": user_record["id"],
                "username": user_record["username"],
                "role": user_record["role"],
                "uri": user_record["uri"],
            }
    return None


@asynccontextmanager
async def get_auth_connection(username: str, password: str):
    """Legacy fallback: open a direct PostgreSQL connection as the target user.

    Used only for accounts where ``sensorthings."User"."password" IS NULL``
    (i.e. accounts created before migration 002 was applied).  A successful
    connection proves the user knows their old credential; the caller is
    responsible for immediately migrating the hash to the application layer.

    Yields the connection on success, or ``None`` if the password is wrong.
    All other errors are re-raised as ``HTTPException 503``.
    """
    connection = None
    try:
        connection = await asyncpg.connect(
            user=username,
            password=password,
            database=POSTGRES_DB,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            timeout=5.0,
            command_timeout=10.0,
        )
        yield connection
    except asyncpg.InvalidPasswordError:
        yield None
    except asyncpg.TooManyConnectionsError:
        logger.error("Database connection limit reached during legacy auth for %r", username)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable.",
        )
    except (asyncpg.PostgresConnectionError, asyncpg.PostgresIOError) as exc:
        logger.error("DB connection error during legacy auth for %r: %s", username, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable.",
        )
    except Exception as exc:
        logger.error("Unexpected error during legacy auth for %r: %s", username, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable.",
        )
    finally:
        if connection is not None:
            try:
                await connection.close()
            except Exception as exc:
                logger.error("Error closing legacy auth connection: %s", exc)


async def authenticate_user(username: str, password: str):
    """Authenticate a local istSOS user.

    Flow
    ----
    1. Fetch ``id, role, password, auth_provider`` from ``sensorthings."User"``.
       Return ``None`` if the user does not exist.
    2. OIDC guard: if ``auth_provider IS NOT NULL``, return ``None`` — federated
       users must authenticate through their identity provider.
    3. If ``password`` column IS NOT NULL (modern account): verify with
       ``pwd_context.verify()``.  Return the token payload on success, ``None``
       on failure.
    4. If ``password`` column IS NULL (legacy account): fall back to
       ``get_auth_connection()`` against PostgreSQL's ``pg_authid``.  On
       success, immediately write the bcrypt hash to ``User.password`` so the
       next login skips the fallback.  Return the token payload.  On failure,
       return ``None``.

    Args:
        username: The login username.
        password: The plaintext password supplied by the user.

    Returns:
        ``{"sub": username, "role": role}`` on success, ``None`` on any failure.
    """
    # ------------------------------------------------------------------
    # 1. Fetch user row
    # ------------------------------------------------------------------
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, role, password, auth_provider
            FROM sensorthings."User"
            WHERE username = $1
            """,
            username,
        )

    if row is None:
        logger.debug("authenticate_user: username %r not found.", username)
        return None

    # ------------------------------------------------------------------
    # 2. OIDC guard — federated users must use their IdP
    # ------------------------------------------------------------------
    if row["auth_provider"] is not None:
        logger.debug(
            "authenticate_user: username %r is a federated user (provider=%r).",
            username, row["auth_provider"],
        )
        return None

    stored_hash = row["password"]

    # ------------------------------------------------------------------
    # 3. Modern account — bcrypt hash present
    # ------------------------------------------------------------------
    if stored_hash is not None:
        if pwd_context.verify(password, stored_hash):
            return {"sub": username, "role": row["role"]}
        logger.debug("authenticate_user: wrong password for username %r.", username)
        return None

    # ------------------------------------------------------------------
    # 4. Legacy account — password IS NULL, fall back to pg_authid
    # ------------------------------------------------------------------
    logger.info(
        "authenticate_user: username %r has no bcrypt hash — using legacy fallback.",
        username,
    )
    async with get_auth_connection(username, password) as legacy_conn:
        if legacy_conn is None:
            # Wrong password for the legacy account
            return None

    # Fallback succeeded: migrate the credential to the application layer
    # so subsequent logins use bcrypt directly (JIT upgrade).
    new_hash = pwd_context.hash(password)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                'UPDATE sensorthings."User" SET password = $1 WHERE id = $2',
                new_hash,
                row["id"],
            )
        logger.info(
            "authenticate_user: JIT credential upgrade complete for username %r.",
            username,
        )
    except Exception as exc:
        # Non-fatal: log and continue — the user is still authenticated.
        logger.error(
            "authenticate_user: failed to write bcrypt hash for username %r: %s",
            username, exc,
        )

    return {"sub": username, "role": row["role"]}


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt, int(expire.timestamp())


def create_refresh_token(payload: dict):
    return create_access_token(
        data={"sub": payload.get("sub"), "role": payload.get("role")}
    )


def decode_token(token: str):
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Decode the Bearer JWT and return the authenticated user dict.

    Raises:
        401 – token missing / invalid / revoked.
        403 – user exists but is in the 'pending' waiting room; they must be
              activated by an administrator before accessing any resource.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        if REDIS and redis.get(token) is not None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
    except InvalidTokenError:
        raise credentials_exception

    # NOTE: role is intentionally fetched live from the DB on every request.
    # This ensures role changes (via PATCH /Users/{id}/role) take effect
    # immediately without requiring JWT rotation, eliminating stale JWT
    # vulnerabilities.
    user = await get_user_from_db(username)
    if user is None:
        raise credentials_exception

    # Pending users have authenticated successfully (their JWT is valid) but
    # they have NO database role and are awaiting admin activation.  Block
    # them here so they never reach any business-logic handler.
    if user["role"] == PENDING_ROLE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account pending admin activation",
        )

    return user
