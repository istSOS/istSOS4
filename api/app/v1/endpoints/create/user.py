import json

from app import HOSTNAME, POSTGRES_PORT_WRITE, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool, get_pool_w
from fastapi import APIRouter, Body, Depends, Request, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

PAYLOAD_EXAMPLE = {
    "username": "carol_williams",
    "password": "P@ssword789",
    "role": "viewer",
    "uri": "https://orcid.org/0000-0004-3456-7890",
}


@v1.api_route(
    "/Users",
    methods=["POST"],
    tags=["Users"],
    summary="Create a new User",
    description="Create a new User entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_user(
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not isinstance(payload, dict):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "code": 400,
                    "type": "error",
                    "message": "Invalid payload format. Expected a dictionary.",
                },
            )

        async with pgpool.acquire() as conn:
            async with conn.transaction():
                if (
                    "username" not in payload
                    or "password" not in payload
                    or "role" not in payload
                ):
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "code": 400,
                            "type": "error",
                            "message": "Missing required properties 'username' or 'password' or 'role'.",
                        },
                    )

                password = payload.pop("password", None)

                for key in list(payload.keys()):
                    if isinstance(payload[key], dict):
                        payload[key] = json.dumps(payload[key])

                keys = ", ".join(f'"{key}"' for key in payload.keys())
                values_placeholders = ", ".join(
                    (f"${i+1}") for i in range(len(payload))
                )
                query = f"""
                    INSERT INTO sensorthings."User" ({keys})
                    VALUES ({values_placeholders})
                    RETURNING id, username;
                """
                user = await conn.fetchrow(query, *payload.values())

                if not payload.get("uri"):
                    query = """
                        UPDATE sensorthings."User"
                        SET uri = $1 || '/Users(' || sensorthings."User".id || ')'
                        WHERE sensorthings."User".id = $2;
                    """
                    await conn.execute(
                        query, f"{HOSTNAME}{SUBPATH}{VERSION}", user["id"]
                    )

                query = "CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}';"
                await conn.execute(
                    query.format(username=user["username"], password=password)
                )

                query = "GRANT sensorthings_{role} to {username};"
                await conn.execute(
                    query.format(
                        role=payload["role"], username=user["username"]
                    )
                )

                return JSONResponse(
                    status_code=status.HTTP_201_CREATED,
                    content={"id": user["id"], "username": user["username"]},
                )

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
