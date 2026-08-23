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

import re

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.v1.endpoints.exceptions import BadRequest, Conflict
from app.v1.endpoints.functions import set_role
from app.v1.endpoints.openapi_responses import merge
from asyncpg.exceptions import DuplicateObjectError, InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

_UNSAFE_POLICY_TOKENS_RE = re.compile(r";|--|/\*|\*/|\x00")
_VALID_OPERATION_KEYS = {"select", "insert", "update", "delete"}

PAYLOAD_EXAMPLE = {
    "users": ["cp1"],
    "name": "test",
    "permissions": {
        "type": "custom",
        "policy": {
            "datastream": {
                "select": """
                    network = 'IDROLOGIA'
                """,
            },
        },
    },
}


@v1.api_route(
    "/Policies",
    methods=["POST"],
    tags=["Policies"],
    summary="Create a Policy",
    description=(
        "Create a named, hand-specified row-level-security policy for the "
        "given users. `permissions.type` must be `custom` -- viewer, "
        "editor, obs_manager, sensor and qc all receive row-level access "
        "automatically from a static policy the moment their role is set "
        "(see `007_session_scoped_rls_policies.sql`), so this endpoint has "
        "nothing left to do for them."
    ),
    status_code=status.HTTP_201_CREATED,
    responses=merge(
        {
            201: {"description": "Created. Response body is empty."},
            # This handler's own `except Exception as e:` catches
            # everything -- including BadRequest and Conflict, which are
            # STAError subclasses meant to reach the app-level handler and
            # produce {"code","type","message"} with their own status
            # (409 for Conflict). Caught here first, every failure path
            # -- malformed payload, a role covered by a static policy
            # already, a duplicate policy -- is flattened to 400 with a
            # plain {"message"} body instead.
            400: {
                "description": (
                    "Malformed payload, `permissions.type` is not "
                    "`custom`, or a user already has a policy. All "
                    "collapse to 400 here regardless of their semantic "
                    "cause -- see the code comment above this response."
                ),
                "content": {
                    "application/json": {
                        "example": {"message": "User cp1 has already a policy."}
                    }
                },
            },
            403: {
                "description": "The caller is not an administrator.",
                "content": {
                    "application/json": {"example": {"message": "Insufficient privileges."}}
                },
            },
            409: {
                "description": "The named policy already exists at the database level.",
                "content": {
                    "application/json": {"example": {"message": "Policy already exists."}}
                },
            },
        }
    ),
)
async def create_policy(
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not isinstance(payload, dict):
            raise BadRequest("Payload must be a dictionary.")

        async with pgpool.acquire() as connection:
            async with connection.transaction():
                role_switched = False
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

                    await set_role(connection, current_user)
                    role_switched = True

                permission_type = payload["permissions"].get("type")

                # viewer/editor/obs_manager/sensor/qc access is granted by
                # the static, group-scoped policies from
                # 007_session_scoped_rls_policies.sql the moment a user's
                # role is set -- that migration DROPs the per-user
                # viewer_policy()/editor_policy()/obs_manager_policy()/
                # sensor_policy()/qc_policy() functions this endpoint used
                # to call, since there is nothing left for them to do.
                # 'custom' is the only type this endpoint still creates.
                if permission_type != "custom":
                    raise BadRequest(
                        "permissions.type must be 'custom'. viewer, editor, "
                        "obs_manager, sensor and qc all receive row-level "
                        "access automatically from a static policy as soon "
                        "as the role is set -- no explicit policy is "
                        "needed, or supported, for them."
                    )

                for user in payload["users"]:
                    query = """
                        SELECT COUNT(*)
                        FROM pg_policies
                        WHERE $1 = ANY (roles)
                    """
                    result = await connection.fetchval(query, user)
                    if result > 0:
                        raise Conflict(
                            f"User {user} has already a policy."
                        )

                await create_policies(
                    connection,
                    payload["users"],
                    payload["permissions"]["policy"],
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
            status_code=status.HTTP_403_FORBIDDEN,
            content={"message": "Insufficient privileges."},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )


async def create_policies(connection, users, policies, name):
    def quote_identifier(value: str) -> str:
        if not isinstance(value, str) or value.strip() == "":
            raise ValueError("Invalid SQL identifier")
        return '"' + value.replace('"', '""') + '"'

    def validate_policy_expression(value: str) -> str:
        if not isinstance(value, str):
            raise ValueError("Policy condition must be a string")
        expression = value.strip()
        if expression == "":
            raise ValueError("Policy condition must not be empty")
        if _UNSAFE_POLICY_TOKENS_RE.search(expression):
            raise ValueError("Unsafe policy condition")
        return expression

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

    if not isinstance(users, list) or len(users) == 0:
        raise ValueError("Users list must not be empty")

    if not isinstance(policies, dict) or len(policies) == 0:
        raise ValueError("Policies must not be empty")

    quoted_users = ", ".join(quote_identifier(user) for user in users)

    for table_key, operations in policies.items():
        table = table_mapping.get(table_key)
        if table is None:
            raise ValueError(f"Unsupported table key: {table_key}")

        if not isinstance(operations, dict) or len(operations) == 0:
            raise ValueError(f"No operations provided for table: {table_key}")

        safe_table = quote_identifier(table)

        for operation, condition in operations.items():
            operation_lc = operation.lower()
            if operation_lc not in _VALID_OPERATION_KEYS:
                raise ValueError(f"Unsupported operation: {operation}")

            safe_name = quote_identifier(
                f"{name}_{table.lower()}_{operation_lc}"
            )
            safe_condition = validate_policy_expression(condition)

            if operation_lc in {"select", "delete"}:
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR {operation_lc.upper()}
                    TO {quoted_users}
                    USING ({safe_condition});
                """
            elif operation_lc == "insert":
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR INSERT
                    TO {quoted_users}
                    WITH CHECK ({safe_condition});
                """
            else:
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR {operation_lc.upper()}
                    TO {quoted_users}
                    USING ({safe_condition})
                    WITH CHECK ({safe_condition});
                """

            await connection.execute(query)
