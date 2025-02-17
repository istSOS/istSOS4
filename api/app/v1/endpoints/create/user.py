import json

from app import HOSTNAME, POSTGRES_PORT_WRITE, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.create.policy import create_policies
from app.v1.endpoints.functions import insert_commit, set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UniqueViolationError
from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {
    "username": "cp1",
    "password": "qwertz",
    "uri": "https://orcid.org/0000-0004-3456-7890",
    "permissions": {
        "type": "viewer",
        "policy": {
            "location": {
                "select": """
                    id IN (
                        SELECT DISTINCT tl.location_id
                        FROM sensorthings."Thing_Location" tl
                        WHERE tl.location_id = sensorthings."Location".id AND tl.thing_id IN (
                            SELECT DISTINCT d.thing_id
                            FROM sensorthings."Datastream" d
                            WHERE d.thing_id = tl.thing_id
                        )
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "thing": {
                "select": """
                    id IN (
                        SELECT DISTINCT d.thing_id
                        FROM sensorthings."Datastream" d
                        WHERE d.thing_id = sensorthings."Thing".id
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "historicallocation": {
                "select": """
                    id IN (
                        SELECT DISTINCT t.id
                        FROM sensorthings."Thing" t
                        WHERE t.id = sensorthings."HistoricalLocation".thing_id
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "observedproperty": {
                "select": """
                    id IN (
                        SELECT DISTINCT d.observedproperty_id
                        FROM sensorthings."Datastream" d
                        WHERE d.observedproperty_id = sensorthings."ObservedProperty".id
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "sensor": {
                "select": """
                    id IN (
                        SELECT DISTINCT d.sensor_id
                        FROM sensorthings."Datastream" d
                        WHERE d.sensor_id = sensorthings."Sensor".id
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "datastream": {
                "select": """
                    id IN (1, 2, 3) OR current_user::text = (
                        SELECT u.username
                        FROM sensorthings."User" u
                        WHERE u.id = (
                            SELECT c.user_id
                            FROM sensorthings."Commit" c
                            WHERE c.id = sensorthings."Datastream".commit_id
                        )
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "featuresofinterest": {
                "select": """
                    id = (
                        SELECT DISTINCT o.featuresofinterest_id
                        FROM sensorthings."Observation" o
                        WHERE o.featuresofinterest_id = sensorthings."FeaturesOfInterest".id
                )""",
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
            "observation": {
                "select": """
                    datastream_id = (
                        SELECT d.id
                        FROM sensorthings."Datastream" d
                        WHERE d.id = sensorthings."Observation".datastream_id
                    )
                """,
                "insert": "true",
                "update": "true",
                "delete": "true",
            },
        },
    },
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
    current_user=Depends(get_current_user),
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
                    or "permissions" not in payload
                ):
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "code": 400,
                            "type": "error",
                            "message": "Missing required properties: 'username' or 'password' or 'permissions'.",
                        },
                    )

                if current_user is not None:
                    await set_role(conn, current_user)

                password = payload.pop("password", None)

                permissions = payload.pop("permissions", None)
                permission_type = permissions.get("type")

                if permission_type == "sensor":
                    payload["role"] = "istsos_sensor"
                elif permission_type == "obs_manager":
                    payload["role"] = "istsos_obs_manager"
                else:
                    payload["role"] = "istsos_user"

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
                    RETURNING id, username, uri;
                """
                user = await conn.fetchrow(query, *payload.values())

                if not payload.get("uri"):
                    query = """
                        UPDATE sensorthings."User"
                        SET uri = $1 || $2 || $3 ||  '/Users(' || sensorthings."User".id || ')'
                        WHERE sensorthings."User".id = $4;
                    """
                    await conn.execute(
                        query, HOSTNAME, SUBPATH, VERSION, user["id"]
                    )

                if payload["role"] == "istsos_sensor":
                    commit = {
                        "message": "Sensor data",
                        "author": user["uri"],
                        "encodingType": "text/plain",
                        "user_id": user["id"],
                    }
                    await insert_commit(conn, commit, "CREATE")

                if current_user is not None:
                    await conn.execute("RESET ROLE;")

                if payload["role"] == "istsos_obs_manager":
                    payload["role"] = "istsos_sensor"

                query = """
                    CREATE USER "{username}"
                    WITH ENCRYPTED PASSWORD '{password}'
                    IN ROLE {role};
                """
                await conn.execute(
                    query.format(
                        username=user["username"],
                        password=password,
                        role=payload["role"],
                    )
                )

                query = """
                    GRANT "{user}" TO "{role}";
                """
                await conn.execute(
                    query.format(
                        user=payload["username"],
                        role=current_user["username"],
                    )
                )

                if permission_type == "custom":
                    await create_policies(
                        conn, payload["username"], permissions["policy"]
                    )
                elif permission_type == "viewer":
                    await conn.execute(
                        f"SELECT sensorthings.viewer_policy('{payload['username']}');"
                    )
                elif permission_type == "editor":
                    await conn.execute(
                        f"SELECT sensorthings.editor_policy('{payload['username']}');"
                    )
                elif permission_type == "sensor":
                    await conn.execute(
                        f"SELECT sensorthings.sensor_policy('{payload['username']}');"
                    )
                else:
                    await conn.execute(
                        f"SELECT sensorthings.obs_manager_policy('{payload['username']}');"
                    )

        return Response(status_code=status.HTTP_201_CREATED)

    except UniqueViolationError:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"message": "User already exists."},
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
