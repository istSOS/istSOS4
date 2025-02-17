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
from app.oauth import get_current_user
from asyncpg.exceptions import InsufficientPrivilegeError
from asyncpg.types import Range
from dateutil import parser
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .functions import create_entity, set_commit, update_datastream_last_foi_id

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
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
    "/BulkObservations",
    methods=["POST"],
    tags=["Observations"],
    summary="Create multiple Observations",
    description="Create multiple Observations in a single request.",
    status_code=status.HTTP_201_CREATED,
)
async def bulk_observations(
    payload: list = Body(example=PAYLOAD_EXAMPLE),
    commit_message=message,
    current_user=user,
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    try:
        async with pgpool.acquire() as conn:
            async with conn.transaction():
                if current_user is not None:
                    query = 'SET ROLE "{username}";'
                    await conn.execute(
                        query.format(username=current_user["username"])
                    )

                try:
                    commit_id = await set_commit(
                        conn, commit_message, current_user
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
                    if "featureOfInterest" in components:
                        return JSONResponse(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": "This method does not support 'featureOfInterest' in components. It will support in future.",
                            },
                        )
                    try:
                        foi_id = await get_foi_id(
                            datastream_id, conn, commit_id=commit_id
                        )
                        await insertBulkObservation(
                            data_array,
                            conn,
                            foi_id,
                            datastream_id=datastream_id,
                            components=components,
                            commit_id=commit_id,
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
                            status_code=status.HTTP_401_UNAUTHORIZED,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": str(e),
                            },
                        )

                if current_user is not None:
                    await conn.execute("RESET ROLE;")
        return Response(status_code=status.HTTP_201_CREATED)

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def insertBulkObservation(
    payload, conn, foi_id, datastream_id, components=None, commit_id=None
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
        result_time_idx = -1
        if components:
            result_idx = components.index("result")
            ph_idx = components.index("phenomenonTime")
            if components.index("resultTime") > -1:
                result_time_idx = components.index("resultTime")
            if isinstance(payload[0][result_idx], str):
                result_type = 3
                observation_type = "resultString"
            elif isinstance(payload[0][result_idx], bool):
                result_type = 1
                observation_type = "resultBoolean"
            elif isinstance(payload[0][result_idx], dict):
                result_type = 2
                observation_type = "resultJSON"
            else:
                result_type = 0
                observation_type = "resultNumber"
        else:
            result_type = 0
            observation_type = "resultNumber"
            ph_idx = 0

        data = []
        ph_interval = None
        for obs in payload:
            if result_time_idx > -1:
                obs[result_time_idx] = parser.parse(obs[result_time_idx])
            if "/" in obs[ph_idx]:
                ph_time = obs[ph_idx].split("/")
                obs[ph_idx] = Range(
                    ph_time[0],
                    ph_time[1],
                    upper_inc=True,
                )
            else:
                obs[ph_idx] = Range(
                    obs[ph_idx],
                    obs[ph_idx],
                    upper_inc=True,
                )
            if ph_interval is None:
                ph_interval = Range(
                    obs[ph_idx].lower,
                    obs[ph_idx].upper,
                    upper_inc=True,
                )
            else:
                if parser.parse(ph_interval.lower) > parser.parse(
                    obs[ph_idx].lower
                ):
                    ph_interval = Range(
                        obs[ph_idx].lower,
                        ph_interval.upper,
                        upper_inc=True,
                    )
                if parser.parse(ph_interval.upper) < parser.parse(
                    obs[ph_idx].upper
                ):
                    ph_interval = Range(
                        ph_interval.lower,
                        obs[ph_idx].upper,
                        upper_inc=True,
                    )
            obs[ph_idx] = Range(
                parser.parse(obs[ph_idx].lower),
                parser.parse(obs[ph_idx].upper),
                upper_inc=True,
            )

            default_obs = [result_type, datastream_id, foi_id]

            if (VERSIONING or AUTHORIZATION) and commit_id is not None:
                default_obs.append(commit_id)

            data.append(obs + default_obs)
        ph_interval = Range(
            parser.parse(ph_interval.lower),
            parser.parse(ph_interval.upper),
            upper_inc=True,
        )
        cols = [
            "phenomenonTime",
            observation_type,
            "resultType",
            "datastream_id",
            "featuresofinterest_id",
        ]

        if components:
            idx = 0
            for c in components:
                if c == "result":
                    components[idx] = observation_type
                idx += 1

            cols = components + [
                "resultType",
                "datastream_id",
                "featuresofinterest_id",
            ]

        if (VERSIONING or AUTHORIZATION) and commit_id is not None:
            cols.append("commit_id")

        column_names = ", ".join(f'"{col}"' for col in cols)

        values_placeholders = ", ".join(
            f"({', '.join(['$' + str(i + 1 + j * len(data[0])) for i in range(len(data[0]))])})"
            for j in range(len(data))
        )

        query = f"""
            INSERT INTO sensorthings."Observation"
            ({column_names})
            VALUES {values_placeholders};
        """

        flattened_values = [item for row in data for item in row]

        await conn.execute(query, *flattened_values)

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
            ph_interval.lower,
            ph_interval.upper,
            datastream_id,
        )

        await update_datastream_last_foi_id(conn, foi_id, datastream_id)


async def get_foi_id(datastream_id, conn, commit_id=None):
    """
    Retrieve or generate a Feature of Interest (FOI) ID for a given datastream.

    This function checks if a FOI ID is already associated with the location of the
    thing related to the provided datastream. If not, it generates a new FOI,
    inserts it into the database, and updates the location and datastream records
    accordingly.

    Args:
        datastream_id (int): The ID of the datastream for which to retrieve or
                             generate the FOI ID.
        conn (asyncpg.Connection): The database connection object.

    Returns:
        int: The FOI ID associated with the datastream.

    Raises:
        ValueError: If the thing associated with the datastream has no locations.
    """

    async with conn.transaction():
        query_location_from_thing_datastream = f"""
            SELECT
                l.id,
                l.name,
                l.description,
                l."encodingType",
                l.location,
                l.properties,
                l.gen_foi_id
            FROM
                sensorthings."Datastream" d
            JOIN
                sensorthings."Thing" t ON d.thing_id = t.id
            JOIN
                sensorthings."Thing_Location" tl ON tl.thing_id = t.id
            JOIN
                sensorthings."Location" l ON l.ID = tl.location_id
            WHERE
                d.id = {datastream_id}
        """

        result = await conn.fetch(query_location_from_thing_datastream)

        if result:
            (
                location_id,
                name,
                description,
                encoding_type,
                location,
                properties,
                gen_foi_id,
            ) = result[0]

            if gen_foi_id is None:
                foi_payload = {
                    "name": name,
                    "description": description,
                    "encodingType": encoding_type,
                    "feature": location,
                    "properties": properties,
                }

                if (VERSIONING or AUTHORIZATION) and commit_id is not None:
                    foi_payload["commit_id"] = commit_id

                foi_id, _ = await create_entity(
                    conn, "FeaturesOfInterest", foi_payload
                )

                update_query = f"""
                    UPDATE sensorthings."Location" 
                    SET "gen_foi_id" = $1::bigint
                    WHERE id = $2::bigint;
                """
                await conn.execute(update_query, foi_id, location_id)

                await update_datastream_last_foi_id(
                    conn, foi_id, datastream_id
                )

                return foi_id
            else:
                select_query = """
                    SELECT last_foi_id
                    FROM sensorthings."Datastream"
                    WHERE id = $1::bigint;
                """
                last_foi_id = await conn.fetchval(select_query, datastream_id)

                select_query = """
                    SELECT id
                    FROM sensorthings."Observation"
                    WHERE "datastream_id" = $1::bigint
                    LIMIT 1;
                """
                observation_ids = await conn.fetch(select_query, datastream_id)

                if last_foi_id is None or not observation_ids:
                    await update_datastream_last_foi_id(
                        conn, gen_foi_id, datastream_id
                    )

                return gen_foi_id
        else:
            raise ValueError(
                "Can not generate foi for Thing with no locations."
            )
