from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.create.create import (
    insert_historical_location_entity,
    set_commit,
)
from app.v1.endpoints.crud import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {"time": "2015-07-01T00:00:00.000Z", "Thing": {"@iot.id": 1}}

ALLOWED_KEYS = [
    "time",
    "Thing",
    "Locations",
]


@v1.api_route(
    "/HistoricalLocations",
    methods=["POST"],
    tags=["HistoricalLocations"],
    summary="Create a new HistoricalLocation",
    description="Create a new HistoricalLocation entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_historical_location(
    request: Request,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_historical_location_entity(
                    connection, payload, commit_id
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )


PAYLOAD_EXAMPLE_THING = {
    "time": "2015-07-01T00:00:00.000Z",
}


@v1.api_route(
    "/Things({thing_id})/HistoricalLocations",
    methods=["POST"],
    tags=["HistoricalLocations"],
    summary="Create a new HistoricalLocation for a Thing",
    description="Create a new HistoricalLocation entity for a Thing entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_historical_location_for_thing(
    request: Request,
    thing_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE_THING),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if (
            not "content-type" in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        if not thing_id:
            raise Exception("Thing ID is required.")

        payload["Thing"] = {"@id": f"Things({thing_id})"}

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                _, header = await insert_historical_location_entity(
                    connection, payload, commit_id
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
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
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )
