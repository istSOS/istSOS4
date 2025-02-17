import ujson
from app import ANONYMOUS_VIEWER, AUTHORIZATION
from app.db.asyncpg_db import get_pool
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse

from .read import set_role

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION and not ANONYMOUS_VIEWER:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Users",
    methods=["GET"],
    tags=["Users"],
    summary="Get all users",
    description="Returns all the users provided by this api (subject to any parameters set)",
    status_code=status.HTTP_200_OK,
)
async def get_users(
    current_user=user,
    pool=Depends(get_pool),
):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                if current_user is not None:
                    await set_role(conn, current_user)

                query = """
                    SELECT row_to_json(t) AS users
                    FROM (SELECT * FROM sensorthings."User") t;
                """
                users = await conn.fetch(query)

                users = [ujson.loads(record["users"]) for record in users]

                if current_user is not None:
                    await conn.execute("RESET ROLE;")

                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"value": users},
                )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Users not found."},
        )
