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
)
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="Login")


async def get_user_from_db(username: str):
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
            detail="Authentication service temporarily unavailable - too many connections"
        )
    except (asyncpg.PostgresConnectionError, asyncpg.PostgresIOError) as e:
        logger.error(f"Database connection error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable"
        )
    except asyncpg.PostgresError as e:
        logger.error(f"Database error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service error"
        )
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable"
        )
    finally:
        if connection is not None:
            try:
                await connection.close()
            except Exception as e:
                logger.error(f"Error closing authentication connection: {e}")


async def authenticate_user(username: str, password: str):
    """
    Authenticate user using PostgreSQL's built-in authentication.
    
    Uses a context manager to ensure connections are properly closed
    and includes comprehensive error handling.
    """
    # Step 1: Verify credentials with PostgreSQL
    async with get_auth_connection(username, password) as auth_conn:
        if auth_conn is None:
            # Invalid credentials
            return None
    
    # Step 2: Get user role from User table (using connection pool)
    pool = await get_pool()
    try:
        async with pool.acquire() as connection:
            query = 'SELECT role FROM sensorthings."User" WHERE username=$1'
            row = await connection.fetchrow(query, username)

            if not row:
                # User authenticated with PostgreSQL but not in User table
                # This indicates an inconsistent state
                logger.warning(
                    f"User '{username}' authenticated with PostgreSQL "
                    "but not found in User table"
                )
                return None

            return {"sub": username, "role": row["role"]}
    except TypeError as e:
        logger.error(f"Unexpected pool acquire error during authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable",
        )


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
    user = await get_user_from_db(username)
    if user is None:
        raise credentials_exception
    return user
