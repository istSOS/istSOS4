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
    SUBPATH,
    VERSION,
)
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.rbac_roles import DELETED_STATUS, PENDING_ROLE
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)

# Absolute path (not the bare "Login" this used to be) so Swagger UI's
# Authorize dialog posts to the right place regardless of which page it was
# opened from, or whether a reverse proxy rewrites the mount prefix.
# Documentation-only: OAuth2PasswordBearer.__call__ never reads tokenUrl, so
# this cannot change runtime behaviour -- it only affects what appears in
# components.securitySchemes.OAuth2PasswordBearer.flows.password.tokenUrl.
_TOKEN_URL = f"{SUBPATH}{VERSION}/Login"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=_TOKEN_URL)
# Optional variant: auto_error=False means FastAPI will NOT raise 401 when the
# Authorization header is absent.  The token parameter will be None instead,
# allowing get_optional_current_user to return None gracefully.
oauth2_scheme_optional = OAuth2PasswordBearer(
    tokenUrl=_TOKEN_URL, auto_error=False
)


async def get_user_from_db(username: str):
    pool = await get_pool()
    async with pool.acquire() as connection:
        query = """
            SELECT id, username, role, uri, status
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
                "status": user_record["status"],
            }
    return None


@asynccontextmanager
async def get_auth_connection(username: str, password: str):
    """
    Context manager for authentication connections.

    Ensures connection is properly closed even on errors.
    Includes timeout protection and comprehensive error handling.
    """
    connection = None
    try:
        connection = await asyncpg.connect(
            user=username,
            password=password,
            database=POSTGRES_DB,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            timeout=5.0,  # Connection timeout
            command_timeout=10.0,  # Command execution timeout
        )
        yield connection
    except asyncpg.InvalidPasswordError:
        # Invalid credentials - return None to signal auth failure
        yield None
    except asyncpg.TooManyConnectionsError:
        logger.error("Database connection limit reached during authentication")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable - too many connections",
        )
    except (asyncpg.PostgresConnectionError, asyncpg.PostgresIOError) as e:
        logger.error(f"Database connection error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable",
        )
    except asyncpg.PostgresError as e:
        logger.error(f"Database error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service error",
        )
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable",
        )
    finally:
        if connection is not None:
            try:
                await connection.close()
            except Exception as e:
                logger.error(f"Error closing authentication connection: {e}")


async def authenticate_user(username: str, password: str):
    """Authenticate a user against the application-layer credential store.

    Authentication is a three-step process that fully replaces the legacy
    pg_authid-only flow:

    Step 1 — Fetch the user row from sensorthings."User".
        If the user does not exist in the table at all, return None
        immediately.  We do NOT fall through to pg_authid for unknown users.

    Step 2 — Bcrypt verify (modern path, password IS NOT NULL).
        If the ``password`` column contains a hash we verify the supplied
        plaintext against it using passlib/bcrypt.  The verify call is
        dispatched to a thread pool (``asyncio.to_thread``) because bcrypt
        is deliberately CPU-intensive blocking work that must not stall the
        event loop.
        • Match  → return user dict.
        • No match → return None (wrong password).

    Step 3 — pg_authid JIT fallback (legacy path, password IS NULL).
        Accounts created before the application-layer credential migration
        have ``password IS NULL``.  We attempt a raw asyncpg connection to
        let PostgreSQL validate the credentials against pg_authid.
        • Failure → return None.
        • Success → compute a bcrypt hash and backfill it into
          sensorthings."User".password so the *next* login goes through
          Step 2 instead.  The backfill failure is logged but does not
          block the current login (best-effort JIT migration).

    Lazy import note
    ----------------
    ``pwd_context`` is imported inside the function body to avoid a
    circular import: ``oauth.py`` → ``password_crud.py`` →
    ``oauth.get_auth_connection`` → ``oauth.py``.

    Args:
        username: The plaintext username supplied by the login form.
        password: The plaintext password supplied by the login form.

    Returns:
        ``{"sub": username, "role": <role>}`` on success, ``None`` on any
        authentication failure.

    Raises:
        HTTPException 503: If the connection pool is unavailable.
    """
    import asyncio

    # Lazy import to break the oauth ↔ password_crud circular dependency.
    from app.db.password_crud import pwd_context

    # ------------------------------------------------------------------
    # Step 1: Fetch user row from application-layer identity store.
    # ------------------------------------------------------------------
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, username, role, password, status
                FROM sensorthings."User"
                WHERE username = $1
                """,
                username,
            )
    except TypeError as exc:
        logger.error(
            "Pool acquire error during authentication for %r: %s",
            username,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable",
        )

    if row is None:
        # Unknown user — do not fall back to pg_authid.
        logger.warning(
            "Authentication attempt for unknown user %r (not in User table).",
            username,
        )
        return None

    stored_hash = row["password"]

    # ------------------------------------------------------------------
    # Step 2: Bcrypt-first verification (modern path).
    # ------------------------------------------------------------------
    if stored_hash is not None:
        # Offload blocking CPU work to the default thread-pool executor so
        # the async event loop is never stalled by the bcrypt computation.
        verified = await asyncio.to_thread(
            pwd_context.verify, password, stored_hash
        )
        if not verified:
            return None
        if row["status"] == "rejected":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Account application was rejected. Please re-register to apply again.",
            )
        if row["status"] == DELETED_STATUS:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="This account has been deactivated.",
            )
        return {"sub": row["username"], "role": row["role"]}

    # ------------------------------------------------------------------
    # Step 3: pg_authid JIT fallback (legacy path, password IS NULL).
    #         On success backfill the bcrypt hash for future logins.
    # ------------------------------------------------------------------
    async with get_auth_connection(username, password) as auth_conn:
        if auth_conn is None:
            # PostgreSQL rejected the credentials too.
            return None

    # pg_authid accepted the credentials — compute hash and backfill.
    new_hash = await asyncio.to_thread(pwd_context.hash, password)

    try:
        from app import POSTGRES_PORT_WRITE
        from app.db.asyncpg_db import get_pool_w

        write_pool = (
            await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()
        )
        async with write_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE sensorthings."User"
                SET password = $1
                WHERE username = $2
                """,
                new_hash,
                username,
            )
        logger.info(
            "JIT migration: bcrypt hash backfilled for user %r.", username
        )
    except Exception as exc:  # noqa: BLE001 — best-effort, never block login
        logger.error(
            "JIT migration: failed to backfill hash for user %r: %s",
            username,
            exc,
        )

    if row["status"] == "rejected":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account application was rejected. Please re-register to apply again.",
        )
    if row["status"] == DELETED_STATUS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="This account has been deactivated.",
        )
    return {"sub": row["username"], "role": row["role"]}


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
        403 – user's account has been deactivated (DELETE /Users). Checked
              live on every request, same as role -- a still-valid JWT
              issued before deactivation stops working on its very next
              use, no token revocation needed.
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

    if user["status"] == DELETED_STATUS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account has been deactivated",
        )

    return user


async def get_optional_current_user(
    token: str | None = Depends(oauth2_scheme_optional),
):
    """Attempt to decode the Bearer JWT and return the user dict.

    Unlike ``get_current_user`` this dependency NEVER raises an HTTP 401/403.
    Instead it returns ``None`` for any of the following conditions:

    * No ``Authorization`` header present (unauthenticated public request).
    * Token is malformed or has an invalid signature.
    * Token has expired (``jwt.ExpiredSignatureError``).
    * Token has been revoked (present in the Redis blocklist).
    * The ``sub`` claim does not map to any row in ``sensorthings."User"``.
    * The user account exists but is in the ``pending`` waiting room.
    * The user account has been deactivated (DELETE /Users).

    Callers (read endpoints, asyncpg_stream_results) treat ``None`` as the
    signal to fall back to the ``guest`` PostgreSQL role, which activates the
    is_public RLS policy on Datastream / Observation.

    Args:
        token: Bearer token extracted by FastAPI from the Authorization header,
               or ``None`` if the header is absent (auto_error=False).

    Returns:
        Authenticated user dict on success, ``None`` otherwise.
    """
    if token is None:
        return None

    try:
        # Honour the Redis revocation blocklist first.
        if REDIS and redis.get(token) is not None:
            logger.debug(
                "get_optional_current_user: token is revoked — treating as anonymous."
            )
            return None

        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            return None
    except InvalidTokenError:
        # Covers ExpiredSignatureError, DecodeError, InvalidSignatureError, etc.
        logger.debug(
            "get_optional_current_user: invalid/expired token — treating as anonymous."
        )
        return None

    # NOTE: role is fetched live from the DB (same rationale as get_current_user).
    user = await get_user_from_db(username)
    if user is None:
        return None

    # Pending users have no DB role yet — deny silently so they cannot sneak
    # through as guests (they'd receive the guest RLS view, not their own data).
    if user["role"] == PENDING_ROLE:
        logger.debug(
            "get_optional_current_user: pending user %r — treating as anonymous.",
            username,
        )
        return None

    # Same silent-anonymous treatment as pending -- a deactivated user must
    # not sneak through as guest either (they'd get the guest RLS view
    # instead of correctly being denied their own data).
    if user["status"] == DELETED_STATUS:
        logger.debug(
            "get_optional_current_user: deactivated user %r — treating as anonymous.",
            username,
        )
        return None

    return user

