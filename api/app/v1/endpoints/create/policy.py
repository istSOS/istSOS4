# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import DuplicateObjectError, InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {
    "users": ["cp1"],
    "name": "test",
    "permissions": {
        "type": "viewer",  # viewer, editor, obs_manager, sensor, custom
    },
}

# PAYLOAD_EXAMPLE = {
#     "users": ["cp1"],
#     "name": "test",
#     "permissions": {
#         "type": "custom",
#         "policy": {
#             "datastream": {
#                 "select": """
#                     network = 'IDROLOGIA'
#                 """,
#             },
#         },
#     },
# }


@v1.api_route(
    "/Policies",
    methods=["POST"],
    tags=["Policies"],
    summary="Create a Policy",
    description="Create a Policy",
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

        async with pgpool.acquire() as connection:
            async with connection.transaction():
                if (
                    "users" not in payload
                    or "name" not in payload
                    or "permissions" not in payload
                ):
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={
                            "message": "Missing required properties: 'users' or 'name' or 'permissions'."
                        },
                    )

                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                permission_type = payload["permissions"].get("type")

                for user in payload["users"]:
                    query = f"""
                        SELECT COUNT(*)
                        FROM pg_policies
                        WHERE $1 = ANY (roles)
                    """
                    result = await connection.fetchval(query, user)
                    if result > 0:
                        raise Exception(f"User {user} has already a policy.")

                    query = f"""
                        SELECT role
                        FROM sensorthings."User"
                        WHERE username = $1
                    """
                    result = await connection.fetchval(query, user)
                    if (
                        permission_type != "custom"
                        and result != permission_type
                    ):
                        raise Exception(
                            f"User {user} has a different role than the policy type."
                        )

                if permission_type == "custom":
                    await create_policies(
                        connection,
                        payload["users"],
                        payload["permissions"]["policy"],
                        payload["name"],
                    )
                elif permission_type == "viewer":
                    await connection.execute(
                        f"SELECT sensorthings.viewer_policy($1, $2);",
                        payload["users"],
                        payload["name"],
                    )
                elif permission_type == "editor":
                    await connection.execute(
                        f"SELECT sensorthings.editor_policy($1, $2);",
                        payload["users"],
                        payload["name"],
                    )
                elif permission_type == "obs_manager":
                    await connection.execute(
                        f"SELECT sensorthings.obs_manager_policy($1, $2);",
                        payload["users"],
                        payload["name"],
                    )
                elif permission_type == "sensor":
                    await connection.execute(
                        f"SELECT sensorthings.sensor_policy($1, $2);",
                        payload["users"],
                        payload["name"],
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


async def create_policies(connection, users, policies, name):
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
    users = ", ".join(users)
    for table, operations in policies.items():
        table = table_mapping.get(table)

        for operation, condition in operations.items():
            if operation in ["select", "delete"]:
                query = f"""
                    CREATE POLICY "{name}_{table.lower()}_{operation}"
                    ON sensorthings."{table}"
                    FOR {operation}
                    TO "{users}"
                    USING ({condition});
                """
            else:
                if operation == "insert":
                    query = f"""
                        CREATE POLICY "{name}_{table.lower()}_{operation}"
                        ON sensorthings."{table}"
                        FOR {operation}
                        TO "{users}"
                        WITH CHECK ({condition});
                    """
                else:
                    query = f"""
                        CREATE POLICY "{name}_{table.lower()}_{operation}"
                        ON sensorthings."{table}"
                        FOR {operation}
                        TO "{users}"
                        USING ({condition})
                        WITH CHECK ({condition});
                    """
            await connection.execute(query)
