from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.v1.endpoints.crud import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import (
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
    "description": "thing 1",
    "name": "thing name 1",
    "properties": {"reference": "first"},
}


@v1.patch("/Things({thing_id})", tags=["Things"])
async def update_thing(
    thing_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not thing_id:
            raise Exception("Thing ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        allowed_keys = [
            "name",
            "description",
            "properties",
            "Locations",
            "Datastreams",
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
                    "Thing",
                    thing_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_thing_entity(connection, thing_id, payload)

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


async def update_thing_entity(connection, thing_id, payload):
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
                    UPDATE sensorthings."Thing_Location"
                    SET location_id = $1
                    WHERE thing_id = $2;
                """,
                location_id,
                thing_id,
            )
            if check is None:
                await connection.execute(
                    """
                        INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("thing_id", "location_id") DO NOTHING;
                    """,
                    thing_id,
                    location_id,
                )
            historical_location_id = await connection.fetchval(
                """
                    INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                    VALUES ($1)
                    RETURNING id;
                """,
                thing_id,
            )
            await connection.execute(
                """
                    INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                    VALUES ($1, $2)
                    ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING;
                """,
                location_id,
                historical_location_id,
            )
        payload.pop("Locations")

    await handle_nested_entities(
        connection,
        payload,
        thing_id,
        "Datastreams",
        "thing_id",
        "Datastream",
    )

    if payload:
        await update_entity(connection, "Thing", thing_id, payload)
