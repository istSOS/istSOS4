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
from app.v1.endpoints.functions import set_role, update_datastream_observedArea
import asyncpg
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse
from app.v1.endpoints.error_response import error_response

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
    # OpenAPI-only: document the mandatory $filter query parameter, mirroring the
    # collection GET's $filter (read/query_parameters.py: name "$filter", type
    # string, description "A filter query"). Declared via openapi_extra — NOT a
    # Query(...) function param — so the handler keeps reading request.query_params
    # and returns its custom 400 ("$filter is required for collection delete") when
    # $filter is missing. A required Query param would make FastAPI raise 422 at the
    # framework layer, breaking that documented 400 safeguard. required=True reflects
    # that an absent $filter is rejected. Zero runtime/behaviour change.
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
    # --- SAFEGUARD FIRST (before any parsing) -----------------------------
    # SECURITY (deliberate divergence from the FROST "Filtered Delete"
    # extension, which deletes the WHOLE collection when $filter is omitted):
    # istSOS4 NEVER does an unbounded collection delete. A missing $filter is a
    # client error, not a "delete everything" instruction. We inspect the raw
    # query params here, before invoking the translator, so an absent $filter
    # can never reach the delete statement.
    if "$filter" not in request.query_params:
        return error_response(
            status.HTTP_400_BAD_REQUEST,
            "$filter is required for collection delete",
        )

    full_path = request.url.path
    if request.url.query:
        full_path += "?" + request.url.query

    # Reuse the GET filter translator: build a SELECT DISTINCT id query with the
    # SAME $filter parsing + cross-entity semi-join the GET path uses (mirrors
    # read/observation.py calling convert_query). No LIMIT/OFFSET, so the FULL
    # match set is collected — not just one $top page.
    #
    # A malformed/invalid $filter makes the translator raise. We catch that HERE
    # and return 400 — exactly the GET contract (never 500 for a bad filter) —
    # while keeping the DB work in a separate try so that genuine internal
    # errors there still map to 500, not 400.
    try:
        ids_query = sta2rest.STA2REST.convert_filter_to_ids_query(full_path)
    except Exception as e:
        return error_response(status.HTTP_400_BAD_REQUEST, str(e))

    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                matched_ids = [
                    row["id"]
                    for row in await connection.fetch(ids_query)
                ]

                if not matched_ids:
                    # Empty match set is success, not an error: 200 + count 0.
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return JSONResponse(
                        status_code=status.HTTP_200_OK,
                        content={"deleted": 0},
                    )

                # Single bulk DELETE (NOT a per-id loop). RETURNING the
                # datastream_id lets us recompute datastream aggregates once per
                # DISTINCT touched datastream below.
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

                # Post-delete maintenance — aggregated equivalent of the
                # single-entity delete's per-row fix-up, run ONCE per DISTINCT
                # touched datastream. We recompute phenomenonTime/resultTime from
                # the REMAINING observations (FoI variant) and the observedArea.
                # We deliberately do NOT touch FeaturesOfInterest / gen_foi_id:
                # deleting Observations does not delete their FoI.
                for datastream_id in touched_datastreams:
                    await update_datastream_phenomenon_time_from_foi(
                        connection, datastream_id
                    )
                    await update_datastream_observedArea(
                        connection, datastream_id
                    )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"deleted": deleted_count},
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "code": 401,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError):
        # conformance: req/request-data/status-code — DB unavailable is 503.
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database temporarily unavailable",
        )
    except Exception:
        # The $filter was already validated above (bad filter -> 400). Anything
        # reaching here is a genuine internal/DB failure -> 500, no stacktrace.
        return error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error"
        )
