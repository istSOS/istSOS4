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
    user = await authenticate_user(form_data.username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token, expire = create_access_token(data={"sub": user})
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

    redis.set(token, "refreshed", ex=expire)

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
        expire = decode_token(token).get("exp")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token",
        )

    redis.set(token, "logged_out", ex=expire)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Successfully logged out"},
    )
