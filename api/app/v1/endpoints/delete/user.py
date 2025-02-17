from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import (
    DependentObjectsStillExistError,
    InsufficientPrivilegeError,
)
from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()


@v1.api_route(
    "/Users",
    methods=["DELETE"],
    tags=["Users"],
    summary="Delete a User",
    description="Delete a User",
    status_code=status.HTTP_200_OK,
)
async def delete_user(
    user: str = Query(
        alias="user",
        description="The user to delete",
    ),
    current_user=Depends(get_current_user),
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                query = """
                    DELETE FROM sensorthings."User"
                    WHERE username = $1;
                """
                await connection.execute(query, user)

                query = """
                    DROP ROLE IF EXISTS {role};
                """
                await connection.execute(query.format(role=user))

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)
    except DependentObjectsStillExistError as e:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"message": "Dependent objects still exist"},
        )
    except InsufficientPrivilegeError as e:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient privilege"},
        )
    except Exception as e:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
