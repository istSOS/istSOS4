from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import DuplicateObjectError, InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {"username": "cp1", "permissions": {"type": "viewer"}}


@v1.api_route(
    "/Policies",
    methods=["POST"],
    tags=["Policies"],
    summary="Create a new policy",
    description="Create a new policy for a user",
    status_code=status.HTTP_201_CREATED,
)
async def create_policy(
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not isinstance(payload, dict):
            raise Exception("Payload must be a dictionary.")

        async with pgpool.acquire() as conn:
            async with conn.transaction():
                if "username" not in payload or "permissions" not in payload:
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "code": 400,
                            "type": "error",
                            "message": "Missing required properties: 'username' or 'permissions'.",
                        },
                    )

                if current_user is not None:
                    await set_role(conn, current_user)

                permission_type = payload["permissions"].get("type")

                if permission_type == "custom":
                    await create_policies(
                        conn,
                        payload["username"],
                        payload["permissions"]["policy"],
                    )
                elif permission_type == "viewer":
                    await conn.execute(
                        f"SELECT sensorthings.viewer_policy('{payload['username']}');"
                    )
                elif permission_type == "editor":
                    await conn.execute(
                        f"SELECT sensorthings.editor_policy('{payload['username']}');"
                    )
                elif permission_type == "obs_manager":
                    await conn.execute(
                        f"SELECT sensorthings.obs_manager_policy('{payload['username']}');"
                    )
                elif permission_type == "sensor":
                    await conn.execute(
                        f"SELECT sensorthings.sensor_policy('{payload['username']}');"
                    )
        return Response(status_code=status.HTTP_201_CREATED)

    except DuplicateObjectError:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"message": "Policy already exists."},
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )


async def create_policies(conn, username, policies):
    table_mapping = {
        "location": "Location",
        "thing": "Thing",
        "historicallocation": "HistoricalLocation",
        "observedproperty": "ObservedProperty",
        "sensor": "Sensor",
        "datastream": "Datastream",
        "observation": "Observation",
        "featuresofinterest": "FeaturesOfInterest",
    }
    for table, operations in policies.items():
        table = table_mapping.get(table)

        for operation, condition in operations.items():
            if operation in ["select", "delete"]:
                query = f"""
                    CREATE POLICY "{username}_{table.lower()}_{operation}"
                    ON sensorthings."{table}"
                    FOR {operation}
                    TO "{username}"
                    USING ({condition});
                """
            else:
                if operation == "insert":
                    query = f"""
                        CREATE POLICY "{username}_{table.lower()}_{operation}"
                        ON sensorthings."{table}"
                        FOR {operation}
                        TO "{username}"
                        WITH CHECK ({condition});
                    """
                else:
                    query = f"""
                        CREATE POLICY "{username}_{table.lower()}_{operation}"
                        ON sensorthings."{table}"
                        FOR {operation}
                        TO "{username}"
                        USING ({condition})
                        WITH CHECK ({condition});
                    """
            await conn.execute(query)
