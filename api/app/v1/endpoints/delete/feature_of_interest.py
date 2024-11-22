from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.v1.endpoints.crud import (
    get_datastreams_from_foi,
    set_role,
    update_datastream_observedArea,
)
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .delete import (
    delete_entity,
    set_commit,
    unlink_foi_from_location,
    update_datastream_phenomenon_time_from_foi,
)

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")


@v1.api_route(
    "/FeaturesOfInterest({feature_of_interest_id})",
    methods=["DELETE"],
    tags=["FeaturesOfInterest"],
    summary="Delete FeatureOfInterest",
)
async def delete_feature_of_interest(
    feature_of_interest_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not feature_of_interest_id:
            raise Exception("FeatureOfInterest ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    "FeaturesOfInterest",
                    feature_of_interest_id,
                )

                datastream_records = await get_datastreams_from_foi(
                    connection, feature_of_interest_id
                )

                for record in datastream_records:
                    ds_id = record["datastream_id"]
                    await update_datastream_observedArea(
                        connection, ds_id, feature_of_interest_id
                    )

                await unlink_foi_from_location(
                    connection, feature_of_interest_id
                )

                await delete_entity(
                    connection, "FeaturesOfInterest", feature_of_interest_id
                )

                for record in datastream_records:
                    ds_id = record["datastream_id"]
                    await update_datastream_phenomenon_time_from_foi(
                        connection, ds_id
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
