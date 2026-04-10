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

from app import (
    LOGIN_BLOCK_SECONDS,
    LOGIN_IP_MAX_ATTEMPTS,
    LOGIN_MAX_ATTEMPTS,
    LOGIN_RATE_LIMIT_ENABLED,
    LOGIN_WINDOW_SECONDS,
    REDIS,
)
from app.db.redis_db import redis
from app.login_security import LoginRateLimiter, emit_login_audit
from app.oauth import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

v1 = APIRouter()
login_rate_limiter = LoginRateLimiter(
    max_attempts=LOGIN_MAX_ATTEMPTS,
    window_seconds=LOGIN_WINDOW_SECONDS,
    block_seconds=LOGIN_BLOCK_SECONDS,
    ip_max_attempts=LOGIN_IP_MAX_ATTEMPTS,
)


@v1.api_route(
    "/Login",
    methods=["POST"],
    tags=["Login"],
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    username = form_data.username.strip()
    client_ip = request.client.host if request.client else "unknown"

    if LOGIN_RATE_LIMIT_ENABLED:
        allowed, retry_after = login_rate_limiter.check(username, client_ip)
        if not allowed:
            emit_login_audit(
                username=username,
                client_ip=client_ip,
                status="throttled",
                detail="too-many-attempts",
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again later.",
                headers={"Retry-After": str(retry_after)},
            )

    user_data = await authenticate_user(username, form_data.password)
    if not user_data:
        if LOGIN_RATE_LIMIT_ENABLED:
            login_rate_limiter.register_failure(
                username,
                client_ip,
            )

        emit_login_audit(
            username=username,
            client_ip=client_ip,
            status="failed",
            detail="invalid-credentials",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if LOGIN_RATE_LIMIT_ENABLED:
        login_rate_limiter.register_success(username, client_ip)

    emit_login_audit(
        username=username,
        client_ip=client_ip,
        status="success",
        detail="token-issued",
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
async def refresh_token(authorization=Header()):
    prefix = "Bearer "
    if not authorization.lower().startswith(prefix.lower()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header format",
        )
    token = authorization[len(prefix) :].strip()

    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    expire = payload.get("exp")

    if REDIS:
        redis.set(token, "refreshed", exat=payload.get("exp"))

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
async def logout(authorization=Header()):
    prefix = "Bearer "
    if not authorization.lower().startswith(prefix.lower()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header format",
        )
    token = authorization[len(prefix) :].strip()

    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    if REDIS:
        redis.set(token, "logged_out", exat=payload.get("exp"))

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Successfully logged out"},
    )
