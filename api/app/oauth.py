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

Consequently, all local credential verification is handled on the Python
side using ``passlib`` (bcrypt), reading the ``password`` column from the
``sensorthings."User"`` table.  There is no ``asyncpg.connect()``-as-user
or ``pg_authid`` involvement in the login flow.

OIDC users (``auth_provider IS NOT NULL``) have a NULL ``password`` column
and are authenticated exclusively through their external identity provider.
The ``/Login`` endpoint is only for local (non-OIDC) istSOS credentials.
"""

import logging
from datetime import datetime, timedelta, timezone

import jwt
from app import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    ALGORITHM,
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


async def authenticate_user(username: str, password: str):
    """Authenticate a local istSOS user against the application-level credential.

    Verification is performed entirely on the Python side using ``passlib``
    (bcrypt).  The stored ``password`` column in ``sensorthings."User"``
    holds the bcrypt hash set at registration time.

    Accounts with a NULL ``password`` column (OIDC users, or users created
    before migration 002 was applied) cannot authenticate via this endpoint
    and will receive ``None`` (→ HTTP 401 at the call site).

    Args:
        username: The login username.
        password: The plaintext password supplied by the user.

    Returns:
        ``{"sub": username, "role": role}`` on success, or ``None`` on any
        authentication failure (unknown user, NULL hash, wrong password).
    """
    pool = await get_pool()
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT id, role, password
            FROM sensorthings."User"
            WHERE username = $1
            """,
            username,
        )

    if row is None:
        # User not found — return None to produce a generic 401 at call site.
        logger.debug("authenticate_user: username %r not found.", username)
        return None

    stored_hash = row["password"]
    if stored_hash is None:
        # OIDC user or pre-migration account — no local credential set.
        logger.debug(
            "authenticate_user: username %r has no local credential (NULL hash).",
            username,
        )
        return None

    if not pwd_context.verify(password, stored_hash):
        # Wrong password — return None; the call site emits HTTP 401.
        logger.debug("authenticate_user: wrong password for username %r.", username)
        return None

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
