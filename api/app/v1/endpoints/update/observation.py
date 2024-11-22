from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import handle_datetime_fields, handle_result_field
from app.v1.endpoints.crud import set_role, update_datastream_observedArea
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .update import (
    handle_associations,
    set_commit,
    update_entity,
    validate_payload_keys,
)

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = {
    "phenomenonTime": "2015-03-03T00:00:00Z",
    "resultTime": "2015-03-03T00:00:00Z",
    "result": 3,
    "resultQuality": "100",
}


@v1.api_route(
    "/Observations({observation_id})", methods=["PATCH"], tags=["Observations"]
)
async def update_observation(
    observation_id: int,
    payload: dict = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        if not observation_id:
            raise Exception("Observation ID not provided")

        if not payload:
            return Response(status_code=status.HTTP_200_OK)

        allowed_keys = [
            "phenomenonTime",
            "result",
            "resultTime",
            "resultQuality",
            "validTime",
            "parameters",
            "Datastream",
            "FeatureOfInterest",
        ]
        validate_payload_keys(payload, allowed_keys)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    "Observations",
                    observation_id,
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                updated = await update_observation_entity(
                    connection, observation_id, payload
                )

                if updated:
                    obs_phenomenon_time = updated["phenomenonTime"]
                    datastream_id = updated["datastream_id"]

                    datastream_query = """
                        SELECT "phenomenonTime"
                        FROM sensorthings."Datastream"
                        WHERE id = $1;
                    """
                    datastream_phenomenon_time = await connection.fetchval(
                        datastream_query, datastream_id
                    )
                    if datastream_phenomenon_time and obs_phenomenon_time:
                        obs_lower = obs_phenomenon_time.lower
                        obs_upper = obs_phenomenon_time.upper
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def update_observation_entity(connection, observation_id, payload):
    handle_datetime_fields(payload)

    handle_result_field(payload)

    handle_associations(payload, ["Datastream", "FeatureOfInterest"])

    if payload:
        await update_entity(connection, "Observation", observation_id, payload)
