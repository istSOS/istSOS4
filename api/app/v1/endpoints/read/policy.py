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

import ujson
from app import ANONYMOUS_VIEWER, AUTHORIZATION
from app.db.asyncpg_db import get_pool
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, Query, status
from fastapi.responses import JSONResponse

from .read import set_role

v1 = APIRouter()
user = Header(default=None, include_in_schema=False)

if AUTHORIZATION and not ANONYMOUS_VIEWER:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Policies",
    methods=["GET"],
    tags=["Policies"],
    summary="Get Policies",
    description="Get Policies",
    status_code=status.HTTP_200_OK,
)
async def get_policies(
    user: str = Query(
        None,
        alias="user",
        description="The user to get the policies for",
    ),
    policy: str = Query(
        None,
        alias="policy",
        description="The name of the policy to get",
    ),
    table: str = Query(
        None,
        alias="table",
        description="The table of the policy to get (Location, Thing, HistoricalLocation, Sensor, ObservedProperty, Datastream, FeaturesOfInterest, Observation)",
    ),
    operation: str = Query(
        None,
        alias="operation",
        description="The operation of the policy to get (SELECT, INSERT, UPDATE, DELETE, ALL)",
    ),
    current_user=user,
    pool=Depends(get_pool),
):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    if current_user["role"] != "administrator":
                        raise InsufficientPrivilegeError

                    await set_role(connection, current_user)

                params = []
                conditions = []

                if user is not None:
                    conditions.append(f"${len(params) + 1} = ANY (roles)")
                    params.append(user)

                if policy is not None:
                    conditions.append(f"policyname = ${len(params) + 1}")
                    params.append(str(policy))

                if table is not None:
                    conditions.append(f"tablename = ${len(params) + 1}")
                    params.append(table)

                if operation is not None:
                    conditions.append(f"cmd = ${len(params) + 1}")
                    params.append(operation)

                query = """
                    SELECT row_to_json(t) AS policies
                    FROM (
                        SELECT * FROM pg_policies
                        WHERE 1 = 1
                        {}
                    ) t
                """.format(
                    " AND " + " AND ".join(conditions) if conditions else ""
                )

                policies = await connection.fetch(query, *params)

                policies = [
                    ujson.loads(record["policies"]) for record in policies
                ]

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"value": policies}
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
