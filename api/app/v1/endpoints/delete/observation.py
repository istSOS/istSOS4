from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.v1.endpoints.functions import set_role, update_datastream_observedArea
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .functions import (
    delete_entity,
    set_commit,
    update_datastream_phenomenon_time,
)

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")


@v1.api_route(
    "/Observations({observation_id})",
    methods=["DELETE"],
    tags=["Observations"],
    summary="Delete an Observation",
    description="Delete an Observation by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_observation(
    observation_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not observation_id:
            raise Exception("Observation ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    "Observation",
                    observation_id,
                )

                id_deleted = None
                deleted = await delete_entity(
                    connection, "Observation", observation_id, True
                )

                if deleted:
                    id_deleted = deleted["id"]
                    obs_phenomenon_time = deleted["phenomenonTime"]
                    datastream_id = deleted["datastream_id"]

                    await update_datastream_phenomenon_time(
                        connection, obs_phenomenon_time, datastream_id
                    )

                    await update_datastream_observedArea(
                        connection, datastream_id
                    )

                if id_deleted is None:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": f"Observation with id {observation_id} not found",
                        },
                    )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "code": 401,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
