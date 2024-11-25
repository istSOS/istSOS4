from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.crud import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import set_commit, update_location_entity

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "description": "location 1",
    "name": "location name 1",
    "location": {"type": "Point", "coordinates": [-117.05, 51.05]},
    "encodingType": "application/vnd.geo+json",
}

ALLOWED_KEYS = [
    "name",
    "description",
    "encodingType",
    "location",
    "properties",
    "Things",
]


@v1.api_route(
    "/Locations({location_id})",
    methods=["PATCH"],
    tags=["Locations"],
    summary="Update a Location",
    description="Update a Location",
    status_code=status.HTTP_200_OK,
)
async def update_location(
    location_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not location_id:
            raise Exception("Location ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        validate_payload_keys(payload, ALLOWED_KEYS)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    "Location",
                    location_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_location_entity(
                    connection,
                    location_id,
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
