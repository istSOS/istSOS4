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
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Query, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

PAYLOAD_EXAMPLE = {"policy": "true"}


@v1.api_route(
    "/Policies",
    methods=["PATCH"],
    tags=["Policies"],
    summary="Update a policy",
    description="Update a policy for a user",
    status_code=status.HTTP_200_OK,
)
async def update_policy(
    policy_name: str = Query(
        alias="policy_name",
        description="The name of the policy to update",
    ),
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    current_user=Depends(get_current_user),
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if "policy" not in payload:
            raise Exception("Missing required properties: 'policy'")

        async with pgpool.acquire() as connection:
            async with connection.transaction():

                if current_user is not None:
                    await set_role(connection, current_user)

                query = """
                    SELECT tablename, cmd FROM pg_policies
                    WHERE policyname = $1;
                """
                row = await connection.fetchrow(query, policy_name)

                if row is None:
                    raise Exception(f"Policy '{policy_name}' not found.")

                tablename, cmd = row["tablename"], row["cmd"]

                policy_sql = {
                    "SELECT": 'ALTER POLICY {} ON sensorthings."{}" USING ({});'.format(
                        policy_name, tablename, payload["policy"]
                    ),
                    "INSERT": 'ALTER POLICY {} ON sensorthings."{}" WITH CHECK ({});'.format(
                        policy_name, tablename, payload["policy"]
                    ),
                    "UPDATE": 'ALTER POLICY {} ON sensorthings."{}" USING ({}) WITH CHECK ({});'.format(
                        policy_name,
                        tablename,
                        payload["policy"],
                        payload["policy"],
                    ),
                    "DELETE": 'ALTER POLICY {} ON sensorthings."{}" USING ({});'.format(
                        policy_name, tablename, payload["policy"]
                    ),
                    "ALL": 'ALTER POLICY {} ON sensorthings."{}" USING ({}) WITH CHECK ({});'.format(
                        policy_name,
                        tablename,
                        payload["policy"],
                        payload["policy"],
                    ),
                }.get(cmd)

                await connection.execute(policy_sql)

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)

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
