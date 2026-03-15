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

import time

from app import REDIS
from app.db.redis_db import redis
from app.oauth import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

v1 = APIRouter()


def _extract_bearer_token(authorization: str | None) -> str:
    prefix = "Bearer "
    if authorization is None or not authorization.lower().startswith(
        prefix.lower()
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header format",
        )
    return authorization[len(prefix) :].strip()


def _ttl_from_exp(exp: int | float | str | None) -> int:
    if exp is None:
        return 1
    try:
        return max(int(exp) - int(time.time()), 1)
    except (TypeError, ValueError):
        return 1


@v1.api_route(
    "/Login",
    methods=["POST"],
    tags=["Login"],
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    user_data = await authenticate_user(form_data.username, form_data.password)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token, expire = create_access_token(data=user_data)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": expire,
        },
    )


@v1.api_route(
    "/Refresh",
    methods=["POST"],
    tags=["Login"],
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def refresh_token(authorization: str | None = Header(default=None)):
    token = _extract_bearer_token(authorization)

    if REDIS and redis.get(token) is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    if REDIS:
        expire = payload.get("exp")
        redis.set(token, "refreshed", ex=_ttl_from_exp(expire))

    access_token, expire = create_refresh_token(payload)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": expire,
        },
    )


@v1.api_route(
    "/Logout",
    methods=["POST"],
    tags=["Login"],
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def logout(authorization: str | None = Header(default=None)):
    token = _extract_bearer_token(authorization)

    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    if REDIS:
        redis.set(token, "logged_out", ex=_ttl_from_exp(expire))

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Successfully logged out"},
    )
