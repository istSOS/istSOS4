from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import handle_datetime_fields
from app.v1.endpoints.crud import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import (
    handle_associations,
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

PAYLOAD_EXAMPLE = {"time": "2015-07-01T00:00:00.000Z"}


@v1.patch(
    "/HistoricalLocations({historical_location_id})",
    tags=["HistoricalLocations"],
    summary="Delete HistoricalLocation",
)
async def update_historical_location(
    historical_location_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not historical_location_id:
            raise Exception("Historical Location ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        allowed_keys = [
            "time",
            "Thing",
            "Locations",
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
                    "HistoricalLocation",
                    historical_location_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_historical_location_entity(
                    connection,
                    historical_location_id,
                    payload,
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


async def update_historical_location_entity(
    connection, historical_location_id, payload
):
    if "Locations" in payload:
        if isinstance(payload["Locations"], dict):
            payload["Locations"] = [payload["Locations"]]
        for location in payload["Locations"]:
            if not isinstance(location, dict) or list(location.keys()) != [
                "@iot.id"
            ]:
                raise Exception(
                    "Invalid format: Each location should be a dictionary with a single key '@iot.id'."
                )
            location_id = location["@iot.id"]
            check = await connection.fetchval(
                """
                    UPDATE sensorthings."Location_HistoricalLocation"
                    SET location_id = $1
                    WHERE historicallocation_id = $2;
                """,
                location_id,
                historical_location_id,
            )
            if check is None:
                await connection.execute(
                    """
                        INSERT INTO sensorthings."Location_HistoricalLocation" ("historicallocation_id", "location_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("historicallocation_id", "location_id") DO NOTHING;
                    """,
                    historical_location_id,
                    location_id,
                )
        payload.pop("Locations")

    handle_datetime_fields(payload)

    handle_associations(payload, ["Thing"])

    if payload:
        await update_entity(
            connection,
            "HistoricalLocation",
            historical_location_id,
            payload,
        )
