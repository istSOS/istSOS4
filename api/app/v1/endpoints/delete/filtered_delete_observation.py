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

from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.sta2rest import sta2rest
from app.v1.endpoints.error_response import error_response
from app.v1.endpoints.functions import set_role, update_datastream_observedArea
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse

from .functions import update_datastream_phenomenon_time_from_foi

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(None, alias="commit-message")


@v1.api_route(
    "/Observations",
    methods=["DELETE"],
    tags=["Observations"],
    summary="Bulk-delete Observations matching a $filter",
    description=(
        "Security-hardened 'Filtered Delete' on /Observations: deletes EXACTLY "
        "the set of Observations a GET /Observations?$filter=<expr> would "
        "return. A $filter is MANDATORY — a request without one is rejected, "
        "never deleting the whole collection."
    ),
    status_code=status.HTTP_200_OK,
    openapi_extra={
        "parameters": [
            {
                "name": "$filter",
                "in": "query",
                "required": True,
                "description": "A filter query",
                "schema": {"type": "string"},
            }
        ]
    },
)
async def delete_observations_filtered(
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    if "$filter" not in request.query_params:
        return error_response(
            status.HTTP_400_BAD_REQUEST,
            "$filter is required for collection delete",
        )

    full_path = request.url.path
    if request.url.query:
        full_path += "?" + request.url.query

    ids_query = sta2rest.STA2REST.convert_filter_to_ids_query(full_path)

    async with pool.acquire() as connection:
        async with connection.transaction():
            if current_user is not None:
                await set_role(connection, current_user)

            matched_ids = [
                row["id"] for row in await connection.fetch(ids_query)
            ]

            if not matched_ids:
                # Empty match set is success, not an error: 200 + count 0.
                if current_user is not None:
                    await connection.execute("RESET ROLE;")
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"deleted": 0},
                )

            deleted_rows = await connection.fetch(
                """
                DELETE FROM sensorthings."Observation"
                WHERE id = ANY($1::bigint[])
                RETURNING datastream_id;
                """,
                matched_ids,
            )

            deleted_count = len(deleted_rows)
            touched_datastreams = {
                row["datastream_id"] for row in deleted_rows
            }

            for datastream_id in touched_datastreams:
                await update_datastream_phenomenon_time_from_foi(
                    connection, datastream_id
                )
                await update_datastream_observedArea(connection, datastream_id)

            if current_user is not None:
                await connection.execute("RESET ROLE;")

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"deleted": deleted_count},
    )
