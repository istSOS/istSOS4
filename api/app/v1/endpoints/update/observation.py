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
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.functions import set_role, update_datastream_observedArea
import asyncpg
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import check_id_exists, set_commit, update_observation_entity
from .json_patch import apply_json_patch_to_entity, normalize_patch_body
from .put import handle_put_replace, request_body_openapi_example

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "phenomenonTime": "2015-03-03T00:00:00Z",
    "resultTime": "2015-03-03T00:00:00Z",
    "result": 3,
    "resultQuality": "100",
}

ALLOWED_KEYS = [
    "phenomenonTime",
    "result",
    "resultTime",
    "resultQuality",
    "validTime",
    "parameters",
    "Datastream",
    "FeatureOfInterest",
]


@v1.api_route(
    "/Observations({observation_id})",
    methods=["PATCH"],
    tags=["Observations"],
    summary="Update an Observation",
    description="Update an Observation",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_observation(
    observation_id: int,
    payload=Depends(normalize_patch_body),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not observation_id:
            raise Exception("Observation ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, "Observation", observation_id
                ):
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Observation not found.",
                        },
                    )

                # req/create-update-delete/update-entity-jsonpatch: resolve an
                # RFC 6902 array body into a merge dict; dict bodies pass through.
                payload = await apply_json_patch_to_entity(
                    connection, "Observation", observation_id, payload
                )

                if not payload:
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return Response(status_code=status.HTTP_200_OK)

                validate_payload_keys(payload, ALLOWED_KEYS)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                updated = await update_observation_entity(
                    connection, observation_id, payload
                )

                if updated:
                    obs_phenomenon_start = updated["phenomenonTimeStart"]
                    obs_phenomenon_end = updated["phenomenonTimeEnd"]
                    obs_result_time = updated["resultTime"]
                    datastream_id = updated["datastream_id"]

                    datastream_query = """
                        SELECT "phenomenonTime", "resultTime"
                        FROM sensorthings."Datastream"
                        WHERE id = $1;
                    """
                    datastream_times = await connection.fetchrow(
                        datastream_query, datastream_id
                    )
                    datastream_phenomenon_time = datastream_times[
                        "phenomenonTime"
                    ]
                    datastream_result_time = datastream_times["resultTime"]
                    if datastream_phenomenon_time and (
                        obs_phenomenon_start is not None
                        and obs_phenomenon_end is not None
                    ):
                        obs_lower = obs_phenomenon_start
                        obs_upper = obs_phenomenon_end
                        datastream_lower = datastream_phenomenon_time.lower
                        datastream_upper = datastream_phenomenon_time.upper
                        if (
                            obs_lower < datastream_lower
                            or obs_upper > datastream_upper
                        ):
                            new_lower_bound = min(
                                obs_lower,
                                datastream_lower,
                            )
                            new_upper_bound = max(
                                obs_upper,
                                datastream_upper,
                            )
                            update_datastream_query = """
                                UPDATE sensorthings."Datastream"
                                SET "phenomenonTime" = tstzrange($1, $2, '[]')
                                WHERE id = $3;
                            """
                            await connection.execute(
                                update_datastream_query,
                                new_lower_bound,
                                new_upper_bound,
                                datastream_id,
                            )

                    if datastream_result_time and obs_result_time:
                        obs_rt = obs_result_time
                        datastream_rt_lower = datastream_result_time.lower
                        datastream_rt_upper = datastream_result_time.upper
                        if (
                            obs_rt < datastream_rt_lower
                            or obs_rt > datastream_rt_upper
                        ):
                            new_rt_lower_bound = min(
                                obs_rt,
                                datastream_rt_lower,
                            )
                            new_rt_upper_bound = max(
                                obs_rt,
                                datastream_rt_upper,
                            )
                            update_datastream_query = """
                                UPDATE sensorthings."Datastream"
                                SET "resultTime" = tstzrange($1, $2, '[]')
                                WHERE id = $3;
                            """
                            await connection.execute(
                                update_datastream_query,
                                new_rt_lower_bound,
                                new_rt_upper_bound,
                                datastream_id,
                            )

                if payload.get("featuresofinterest_id"):
                    await update_datastream_observedArea(
                        connection, datastream_id
                    )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)
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
        # conformance: req/request-data/status-code — DB unavailable is 503 (mirror read.py), not 400
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "code": 503,
                "type": "error",
                "message": "Database temporarily unavailable",
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


# conformance: req/create-update-delete/update-entity-put — phenomenonTime,
# resultTime and result are mandatory (resultTime is NOT NULL and a null
# phenomenonTime cannot be stored on the hypertable partition column).
# resultQuality / validTime / parameters are optional and reset to null when a
# PUT omits them. The mandatory Datastream / FeatureOfInterest relations are
# left untouched when absent so the existing, required links are not orphaned.
REQUIRED_PUT_KEYS = ["phenomenonTime", "resultTime", "result"]
OPTIONAL_PUT_KEYS = ["resultQuality", "validTime", "parameters"]


async def _post_update_observation(
    connection, observation_id, payload, updated
):
    """Re-expand the parent Datastream's phenomenonTime/resultTime/observedArea.

    Mirrors the PATCH handler so a PUT maintains the same derived Datastream
    state (req/create-update-delete/update-entity-put).
    """
    datastream_id = None
    if updated:
        obs_phenomenon_start = updated["phenomenonTimeStart"]
        obs_phenomenon_end = updated["phenomenonTimeEnd"]
        obs_result_time = updated["resultTime"]
        datastream_id = updated["datastream_id"]

        datastream_times = await connection.fetchrow(
            """
                SELECT "phenomenonTime", "resultTime"
                FROM sensorthings."Datastream"
                WHERE id = $1;
            """,
            datastream_id,
        )
        datastream_phenomenon_time = datastream_times["phenomenonTime"]
        datastream_result_time = datastream_times["resultTime"]
        if datastream_phenomenon_time and (
            obs_phenomenon_start is not None
            and obs_phenomenon_end is not None
        ):
            datastream_lower = datastream_phenomenon_time.lower
            datastream_upper = datastream_phenomenon_time.upper
            if (
                obs_phenomenon_start < datastream_lower
                or obs_phenomenon_end > datastream_upper
            ):
                await connection.execute(
                    """
                        UPDATE sensorthings."Datastream"
                        SET "phenomenonTime" = tstzrange($1, $2, '[]')
                        WHERE id = $3;
                    """,
                    min(obs_phenomenon_start, datastream_lower),
                    max(obs_phenomenon_end, datastream_upper),
                    datastream_id,
                )

        if datastream_result_time and obs_result_time:
            datastream_rt_lower = datastream_result_time.lower
            datastream_rt_upper = datastream_result_time.upper
            if (
                obs_result_time < datastream_rt_lower
                or obs_result_time > datastream_rt_upper
            ):
                await connection.execute(
                    """
                        UPDATE sensorthings."Datastream"
                        SET "resultTime" = tstzrange($1, $2, '[]')
                        WHERE id = $3;
                    """,
                    min(obs_result_time, datastream_rt_lower),
                    max(obs_result_time, datastream_rt_upper),
                    datastream_id,
                )

    if payload.get("featuresofinterest_id") and datastream_id is not None:
        await update_datastream_observedArea(connection, datastream_id)


@v1.api_route(
    "/Observations({observation_id})",
    methods=["PUT"],
    tags=["Observations"],
    summary="Replace an Observation",
    description="Replace an Observation (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_observation(
    observation_id: int,
    request: Request,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    # conformance: req/create-update-delete/update-entity-put (18-088 §10.3)
    return await handle_put_replace(
        pool=pool,
        request=request,
        entity_db_name="Observation",
        not_found_message="Observation not found.",
        entity_id=observation_id,
        commit_message=commit_message,
        current_user=current_user,
        allowed_keys=ALLOWED_KEYS,
        required_keys=REQUIRED_PUT_KEYS,
        optional_keys=OPTIONAL_PUT_KEYS,
        update_entity_fn=update_observation_entity,
        post_update=_post_update_observation,
    )
