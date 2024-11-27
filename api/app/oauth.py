from datetime import datetime, timedelta, timezone

import jwt
from app import ACCESS_TOKEN_EXPIRE_MINUTES, ALGORITHM, SECRET_KEY
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

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


async def authenticate_user(username: str):
    pool = await get_pool()
    async with pool.acquire() as connection:
        query = """
            SELECT username
            FROM sensorthings."User"
            WHERE username = $1
        """
        user_record = await connection.fetchval(query, username)
        return user_record


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt, int(expire.timestamp())


def create_refresh_token(payload: dict):
    return create_access_token(data={"sub": payload.get("sub")})


def decode_token(token: str):
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        if redis.get(token) is not None:
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
