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
from app.v1.endpoints.exceptions import BadRequest
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
                "select": "name LIKE 'meteo_%'",
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
        "Create a row-level-security policy for the given users.\n\n"
        "`viewer` / `editor` / `obs_manager` / `sensor` / `qc` are already "
        "covered by the static per-role policies from "
        "`006_session_scoped_rls_policies.sql`; there is nothing for this "
        "endpoint to create for them, and it returns 400.\n\n"
        "`custom` builds hand-specified policies from `permissions.policy`. "
        "It applies only to users whose role is `custom` (those have no "
        "blanket grant), and each policy is scoped to the `user` group role "
        "plus an identity check on `current_app_user_id()`, so it works "
        "even though istSOS users are not PostgreSQL roles."
    ),
    status_code=status.HTTP_201_CREATED,
    responses=merge(
        {
            201: {"description": "Custom policy created. Response body is empty."},
            400: {
                "description": (
                    "Malformed payload, unknown `permissions.type`, or "
                    "`permissions.type` is one of the five role types "
                    "already covered by a static RLS policy (nothing to "
                    "create for those)."
                )
            },
            403: {"description": "The caller is not an administrator."},
            409: {"description": "A policy of that name already exists, or the user already has one."},
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

                permission_type = payload["permissions"].get("type")

                # The five role types are already enforced by the static
                # per-role policies from 006_session_scoped_rls_policies.sql,
                # which apply the moment set_role() runs -- there is nothing
                # for this endpoint to create for them. Previously this
                # returned 200 with an explanatory message so the caller
                # would have to read the body to learn nothing happened;
                # 400 makes that failure visible in the status code itself,
                # matching every other "you asked for something invalid"
                # response in this API.
                STATIC_ROLE_TYPES = {
                    "viewer",
                    "editor",
                    "obs_manager",
                    "sensor",
                    "qc",
                }
                if permission_type in STATIC_ROLE_TYPES:
                    raise BadRequest(
                        f"Role '{permission_type}' is covered by a static "
                        "RLS policy already -- there is nothing for "
                        "POST /Policies to create. Only permissions.type "
                        "'custom' creates a policy through this endpoint."
                    )

                if permission_type != "custom":
                    raise BadRequest(
                        "permissions.type must be one of: viewer, editor, "
                        "obs_manager, sensor, qc, custom."
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

    # istSOS users are not PostgreSQL roles, so a policy cannot be scoped
    # ``TO <username>``. Instead every custom policy is scoped to the "user"
    # group role and gated on the caller's application identity, read from
    # the app.current_user_id session claim via current_app_user_id()
    # (the same mechanism 006_session_scoped_rls_policies.sql uses).
    #
    # Only 'custom'-role users are allowed here: they get NO blanket grant
    # from the static policies, so these hand-written rules are their entire
    # access. A viewer/editor already has group-wide access, so a custom
    # policy for them would only ever widen it -- refused.
    rows = await connection.fetch(
        'SELECT username, id, role FROM sensorthings."User" '
        "WHERE username = ANY($1::text[])",
        list(users),
    )
    found = {r["username"]: r for r in rows}
    missing = [u for u in users if u not in found]
    if missing:
        raise ValueError(f"Unknown user(s): {', '.join(missing)}")
    not_custom = [u for u in users if found[u]["role"] != "custom"]
    if not_custom:
        raise ValueError(
            "permissions.type 'custom' only applies to users whose role is "
            f"'custom'. These are not: {', '.join(not_custom)}."
        )

    user_ids = sorted(int(found[u]["id"]) for u in users)
    id_array = "ARRAY[" + ", ".join(str(i) for i in user_ids) + "]::bigint[]"
    # The role is checked live on every query, not only here at creation:
    # a user later moved to viewer/editor must not keep these grants on top
    # of their new role's. The policy stays in place and applies again if
    # the user is moved back to 'custom'.
    identity_clause = (
        "sensorthings.current_app_user_role() = 'custom' AND "
        f"sensorthings.current_app_user_id() = ANY ({id_array})"
    )

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
            predicate = f"{identity_clause} AND ({safe_condition})"

            if operation_lc in {"select", "delete"}:
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR {operation_lc.upper()}
                    TO "user"
                    USING ({predicate});
                """
            elif operation_lc == "insert":
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR INSERT
                    TO "user"
                    WITH CHECK ({predicate});
                """
            else:
                query = f"""
                    CREATE POLICY {safe_name}
                    ON sensorthings.{safe_table}
                    FOR {operation_lc.upper()}
                    TO "user"
                    USING ({predicate})
                    WITH CHECK ({predicate});
                """

            await connection.execute(query)
