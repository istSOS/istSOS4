from datetime import datetime, timedelta, timezone

import jwt
from app.db.asyncpg_db import get_pool
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


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
        return {
            "username": user_record,
        }
    return None


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except InvalidTokenError:
        raise credentials_exception
    user = await get_user_from_db(username)
    if user is None:
        raise credentials_exception
    return user
