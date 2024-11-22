from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import handle_datetime_fields
from app.v1.endpoints.crud import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import (
    handle_associations,
    handle_nested_entities,
    set_commit,
    update_entity,
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
    "unitOfMeasurement": {
        "name": "Lumen",
        "symbol": "lm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
    },
    "description": "datastream 1",
    "name": "datastream name 1",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
}


@v1.patch("/Datastreams({datastream_id})", tags=["Datastreams"])
async def update_datastream(
    datastream_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not datastream_id:
            raise Exception("Datastream ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        allowed_keys = [
            "name",
            "description",
            "unitOfMeasurement",
            "observationType",
            "observedArea",
            "phenomenonTime",
            "resultTime",
            "properties",
            "Thing",
            "Sensor",
            "ObservedProperty",
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
                    "Datastream",
                    datastream_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_datastream_entity(
                    connection,
                    datastream_id,
                    payload,
                )

                if current_user is not None:
                    await set_role(connection, current_user)

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


async def update_datastream_entity(connection, datastream_id, payload):
    handle_datetime_fields(payload)

    handle_associations(payload, ["Thing", "Sensor", "ObservedProperty"])

    await handle_nested_entities(
        connection,
        payload,
        datastream_id,
        "Observations",
        "datastream_id",
        "Observation",
    )

    if payload:
        await update_entity(connection, "Datastream", datastream_id, payload)
