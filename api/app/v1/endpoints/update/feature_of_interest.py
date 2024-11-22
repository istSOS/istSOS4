from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.v1.endpoints.crud import (
    get_datastreams_from_foi,
    set_role,
    update_datastream_observedArea,
)
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import (
    handle_nested_entities,
    set_commit,
    update_entity,
    validate_epsg,
    validate_payload_keys,
)

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "name": "A weather station.",
    "description": "A weather station.",
    "encodingType": "application/vnd.geo+json",
    "feature": {"type": "Point", "coordinates": [-114.05, 51.05]},
}


@v1.patch(
    "/FeaturesOfInterest({feature_of_interest_id})",
    tags=["FeaturesOfInterest"],
    summary="Delete FeatureOfInterest",
)
async def update_feature_of_interest(
    feature_of_interest_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not feature_of_interest_id:
            raise Exception("Feature of Interest ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        allowed_keys = [
            "name",
            "description",
            "encodingType",
            "feature",
            "properties",
            "Observations",
        ]
        validate_payload_keys(payload, allowed_keys)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    "FeatureOfInterest",
                    feature_of_interest_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_feature_of_interest_entity(
                    connection,
                    feature_of_interest_id,
                    payload,
                )

                datastream_records = await get_datastreams_from_foi(
                    connection, feature_of_interest_id
                )
                for record in datastream_records:
                    ds_id = record["datastream_id"]
                    await update_datastream_observedArea(
                        connection, ds_id, feature_of_interest_id
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


async def update_feature_of_interest_entity(
    connection, feature_of_interest_id, payload
):
    if payload["feature"]:
        validate_epsg(payload["feature"])

    await handle_nested_entities(
        connection,
        payload,
        feature_of_interest_id,
        "Observations",
        "featuresofinterest_id",
        "Observation",
    )

    if payload:
        await update_entity(
            connection, "FeaturesOfInterest", feature_of_interest_id, payload
        )
