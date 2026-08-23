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
from app.models.error import MessageError
from app.oauth import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_token,
    oauth2_scheme_optional,
)
from app.v1.endpoints.openapi_responses import (
    BAD_REQUEST_TOKEN_FORMAT,
    LOGIN_FAILED,
    TOKEN_OK,
    TOKEN_REVOKED,
    merge,
    response,
)
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

v1 = APIRouter()


def _extract_bearer_token(authorization: str | None) -> str:
    prefix = "Bearer "
    if not authorization or not authorization.lower().startswith(prefix.lower()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header format",
        )

    return authorization[len(prefix) :].strip()


def ttl_from_exp(exp: int | float | str | None) -> int:
    if exp is None:
        return 1
    try:
        return max(int(exp) - int(time.time()), 1)
    except (TypeError, ValueError):
        return 1


@v1.api_route(
    "/Login",
    methods=["POST"],
    tags=["Authentication"],
    status_code=status.HTTP_200_OK,
    summary="Obtain a bearer token (local accounts)",
    description=(
        "Exchange a username and password for a signed JWT.\n\n"
        "This is the URL the **Authorize** button (top of this page) posts "
        "to -- you normally never call it directly.\n\n"
        "Only **local** accounts authenticate here. An account created "
        "through an external identity provider has no PostgreSQL password "
        "and must use `GET /auth/{provider}/login` instead."
    ),
    responses=merge(TOKEN_OK, LOGIN_FAILED),
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
    tags=["Authentication"],
    status_code=status.HTTP_200_OK,
    summary="Exchange a valid token for a fresh one",
    description=(
        "Issues a new token carrying the same `sub`/`role`, without "
        "requiring the password again. If `REDIS=1` on this deployment, "
        "the old token is marked used and rejected on any further refresh "
        "attempt; without Redis, refreshing has no effect on the old "
        "token's validity."
    ),
    # A bare dependency (not bound to a parameter) so Swagger's padlock
    # covers this route and auto-sends the bearer token on Try it out.
    # oauth2_scheme_optional (auto_error=False) is required here, not
    # oauth2_scheme -- the handler still does its own header parsing and
    # validation below via _extract_bearer_token/decode_token, and using
    # the *required* scheme would make FastAPI raise 401 on a missing
    # header before that code ever ran, turning today's 400 into a 401.
    dependencies=[Depends(oauth2_scheme_optional)],
    responses=merge(
        TOKEN_OK,
        BAD_REQUEST_TOKEN_FORMAT,
        TOKEN_REVOKED,
    ),
)
async def refresh_token(
    # include_in_schema=False: the dependency above already documents this
    # as a bearer-token operation with a padlock; a second, raw
    # "authorization" text field in the parameters list would be redundant
    # and confusing. The handler still reads the real header itself --
    # this only changes what appears in the schema, not what's parsed.
    authorization: str | None = Header(default=None, include_in_schema=False),
):
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
        redis.set(token, "refreshed", ex=ttl_from_exp(expire))

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
    tags=["Authentication"],
    status_code=status.HTTP_200_OK,
    summary="Revoke a token",
    description=(
        "Requires `REDIS=1` on this deployment to actually take effect -- "
        "without Redis this returns 200 but the token remains valid until "
        "it naturally expires.\n\n"
        "Revoking the token you are currently Authorized with in this page "
        "will make every subsequent Try it out call fail until you "
        "Authorize again."
    ),
    dependencies=[Depends(oauth2_scheme_optional)],  # see /Refresh for why
    responses=merge(
        {200: response(MessageError, "Token revoked (or would be, if REDIS=1).",
                        {"message": "Successfully logged out"})},
        BAD_REQUEST_TOKEN_FORMAT,
    ),
)
async def logout(
    authorization: str | None = Header(default=None, include_in_schema=False),
):
    token = _extract_bearer_token(authorization)

    try:
        expire = decode_token(token).get("exp")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    if REDIS:
        redis.set(token, "logged_out", ex=ttl_from_exp(expire))

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Successfully logged out"},
    )
