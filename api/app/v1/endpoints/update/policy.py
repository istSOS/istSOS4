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
from app.utils.utils import pg_quote_ident, validate_payload_keys
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UndefinedObjectError
from fastapi import APIRouter, Body, Depends, Query, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {"users": ["cp1"], "policy": "true"}

ALLOWED_KEYS = [
    "users",
    "policy",
]

_UNSAFE_POLICY_TOKENS_RE = re.compile(r";|--|/\*|\*/|\x00")


def _validate_policy_expression(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Policy expression must be a string")

    expression = value.strip()
    if expression == "":
        raise ValueError("Policy expression must not be empty")

    if _UNSAFE_POLICY_TOKENS_RE.search(expression):
        raise ValueError("Unsafe policy expression")

    return expression


@v1.api_route(
    "/Policies",
    methods=["PATCH"],
    tags=["Policies"],
    summary="Update a Policy",
    description="Update a Policy",
    status_code=status.HTTP_200_OK,
)
async def update_policy(
    policy: str = Query(
        alias="policy",
        description="The name of the policy to update",
    ),
    payload: dict = Body(examples={"default": {"value": PAYLOAD_EXAMPLE}}),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pgpool.acquire() as connection:
            async with connection.transaction():
                role_switched = False
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError
                    await set_role(connection, current_user)
                    role_switched = True

                try:
                    validate_payload_keys(payload, ALLOWED_KEYS)

                    if payload.get("users") is not None:
                        query = """
                            SELECT sensorthings.add_users_to_policy($1, $2);
                        """
                        tablename, cmd = await connection.fetchval(
                            query, payload["users"], policy
                        )
                    else:
                        query = """
                            SELECT tablename, cmd FROM pg_policies
                            WHERE policyname = $1;
                        """
                        row = await connection.fetchrow(query, policy)
                        if row is None:
                            raise Exception(f"Policy '{policy}' not found.")

                        tablename, cmd = row["tablename"], row["cmd"]

                    if payload.get("policy") is not None:
                        policy_expression = _validate_policy_expression(
                            payload["policy"]
                        )
                        policy_ident = pg_quote_ident(policy)
                        table_ident = pg_quote_ident(tablename)
                        cmd_upper = (cmd or "").upper()

                        policy_sql = {
                            "SELECT": f"ALTER POLICY {policy_ident} ON sensorthings.{table_ident} USING ({policy_expression});",
                            "INSERT": f"ALTER POLICY {policy_ident} ON sensorthings.{table_ident} WITH CHECK ({policy_expression});",
                            "UPDATE": f"ALTER POLICY {policy_ident} ON sensorthings.{table_ident} USING ({policy_expression}) WITH CHECK ({policy_expression});",
                            "DELETE": f"ALTER POLICY {policy_ident} ON sensorthings.{table_ident} USING ({policy_expression});",
                            "ALL": f"ALTER POLICY {policy_ident} ON sensorthings.{table_ident} USING ({policy_expression}) WITH CHECK ({policy_expression});",
                        }.get(cmd_upper)

                        if policy_sql is None:
                            raise ValueError(
                                f"Unsupported policy command: {cmd}"
                            )

                        await connection.execute(policy_sql)
                finally:
                    if role_switched:
                        await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)

    except UndefinedObjectError:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Policy not found"},
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