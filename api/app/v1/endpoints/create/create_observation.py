import json
from datetime import datetime

from app import (
    AUTHORIZATION,
    HOSTNAME,
    POSTGRES_PORT_WRITE,
    SUBPATH,
    VERSION,
    VERSIONING,
)
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import handle_datetime_fields, handle_result_field
from app.v1.endpoints.insert import (
    check_iot_id_in_payload,
    check_missing_properties,
    generate_feature_of_interest,
    get_commit,
    handle_associations,
    insertDatastream,
    insertFeaturesOfInterest,
    update_datastream_last_foi_id,
)
from asyncpg.exceptions import InsufficientPrivilegeError
from asyncpg.types import Range
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING:
    message = Header(alias="commit-message")

PAYLOAD_EXAMPLE = [
    {
        "Datastream": {"@iot.id": 1},
        "components": [
            "result",
            "phenomenonTime",
            "resultTime",
            "resultQuality",
        ],
        "dataArray": [
            [
                "1.750000",
                "2023-01-01T00:10:00+01:00",
                "2023-01-01T00:10:00+01:00",
                "100",
            ],
            [
                "1.610000",
                "2023-01-01T00:20:00+01:00",
                "2023-01-01T00:20:00+01:00",
                "100",
            ],
            [
                "1.690000",
                "2023-01-01T00:30:00+01:00",
                "2023-01-01T00:30:00+01:00",
                "100",
            ],
        ],
    }
]


@v1.api_route(
    "/CreateObservations",
    methods=["POST"],
    tags=["Observations"],
    summary="Data Array Extension",
)
async def create_observations(
    payload: list = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        response_urls = []

        async with pool.acquire() as conn:
            async with conn.transaction():
                if current_user is not None:
                    query = 'SET ROLE "{username}";'
                    await conn.execute(
                        query.format(username=current_user["username"])
                    )

                try:
                    commit_id = await get_commit(
                        commit_message, conn, current_user
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

                for observation_set in payload:
                    datastream_id = observation_set.get("Datastream", {}).get(
                        "@iot.id"
                    )
                    components = observation_set.get("components", [])
                    data_array = observation_set.get("dataArray", [])

                    if not datastream_id:
                        return JSONResponse(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": "Missing 'datastream_id' in Datastream.",
                            },
                        )

                    # Check that at least phenomenonTime and result are present
                    if (
                        "phenomenonTime" not in components
                        or "result" not in components
                    ):
                        return JSONResponse(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": "Missing required properties 'phenomenonTime' or 'result' in components.",
                            },
                        )

                    for data in data_array:
                        try:
                            observation_payload = {
                                components[i]: (
                                    data[i] if i < len(data) else None
                                )
                                for i in range(len(components))
                            }

                            observation_payload["datastream_id"] = (
                                datastream_id
                            )

                            if "FeatureOfInterest/id" in observation_payload:
                                observation_payload["FeatureOfInterest"] = {
                                    "@iot.id": observation_payload.pop(
                                        "FeatureOfInterest/id"
                                    )
                                }
                            else:
                                await generate_feature_of_interest(
                                    observation_payload,
                                    conn,
                                    commit_id=commit_id,
                                )

                            _, observation_selfLink = (
                                await insertDataArrayObservation(
                                    observation_payload,
                                    conn,
                                    commit_id=commit_id,
                                )
                            )
                            response_urls.append(observation_selfLink)
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
                            response_urls.append("error")

                if current_user is not None:
                    await conn.execute("RESET ROLE;")
        return JSONResponse(
            status_code=status.HTTP_201_CREATED, content=response_urls
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
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def insertDataArrayObservation(
    payload, conn, datastream_id=None, commit_id=None
):
    """
    Inserts observation data into the database.

    Args:
        payload (dict or list): The payload containing the observation(s) to be inserted.
        conn (connection): The database connection object.
        datastream_id (int, optional): The ID of the datastream associated with the observation. Defaults to None.

    Returns:
        tuple: A tuple containing the ID and selfLink of the inserted observation.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    async with conn.transaction():
        if isinstance(payload, dict):
            payload = [payload]

        observations = []

        all_keys = set()

        for obs in payload:
            if datastream_id:
                obs["datastream_id"] = datastream_id

            await handle_associations(
                obs,
                "Datastream",
                datastream_id,
                insertDatastream,
                conn,
                commit_id=commit_id,
            )

            if "FeatureOfInterest" in obs:
                if "@iot.id" in obs["FeatureOfInterest"]:
                    features_of_interest_id = obs["FeatureOfInterest"][
                        "@iot.id"
                    ]
                    check_iot_id_in_payload(
                        obs["FeatureOfInterest"], "FeatureOfInterest"
                    )
                    select_query = f"""
                        SELECT last_foi_id
                        FROM sensorthings."Datastream"
                        WHERE id = $1::bigint;
                    """
                    last_foi_id = await conn.fetchval(
                        select_query, obs["datastream_id"]
                    )
                    if last_foi_id != features_of_interest_id:
                        await update_datastream_last_foi_id(
                            conn,
                            features_of_interest_id,
                            obs["datastream_id"],
                        )
                else:
                    features_of_interest_id, _ = (
                        await insertFeaturesOfInterest(
                            obs["FeatureOfInterest"],
                            conn,
                            obs["datastream_id"],
                            commit_id=commit_id,
                        )
                    )
                obs.pop("FeatureOfInterest", None)
                obs["featuresofinterest_id"] = features_of_interest_id
            else:
                await generate_feature_of_interest(
                    obs, conn, commit_id=commit_id
                )

            check_missing_properties(obs, ["Datastream", "FeaturesOfInterest"])
            handle_datetime_fields(obs)
            handle_result_field(obs)

            if obs.get("phenomenonTime") is None:
                current_time = datetime.now()
                obs["phenomenonTime"] = Range(
                    current_time,
                    current_time,
                    upper_inc=True,
                )

            for key, value in obs.items():
                if isinstance(value, dict):
                    obs[key] = json.dumps(value)
                all_keys.add(key)

        all_keys = list(all_keys)

        for obs in payload:
            obs_tuple = []
            for key in all_keys:
                obs_tuple.append(obs.get(key))
            observations.append(tuple(obs_tuple))

        keys = ", ".join(f'"{key}"' for key in all_keys)
        values_placeholders = ", ".join(
            f"({', '.join(f'${i * len(all_keys) + j + 1}' for j in range(len(all_keys)))})"
            for i in range(len(observations))
        )

        insert_query = f"""
            INSERT INTO sensorthings."Observation" ({keys})
            VALUES {values_placeholders}
            RETURNING id, lower("phenomenonTime"), upper("phenomenonTime"), datastream_id, featuresofinterest_id;
        """

        values = [
            value for observation in observations for value in observation
        ]
        result = await conn.fetch(insert_query, *values)

        min_phenomenon_times = [record["lower"] for record in result]
        max_phenomenon_times = [record["upper"] for record in result]
        update_query = """
            UPDATE sensorthings."Datastream"
            SET "phenomenonTime" = tstzrange(
                LEAST($1::timestamptz, lower("phenomenonTime")),
                GREATEST($2::timestamptz, upper("phenomenonTime")),
                '[]'
            )
            WHERE id = $3::bigint;
        """
        await conn.execute(
            update_query,
            min(min_phenomenon_times),
            max(max_phenomenon_times),
            result[0]["datastream_id"],
        )

        observation_id = result[0]["id"]
        observation_selfLink = (
            f"{HOSTNAME}{SUBPATH}{VERSION}/Observations({observation_id})"
        )

        return observation_id, observation_selfLink
