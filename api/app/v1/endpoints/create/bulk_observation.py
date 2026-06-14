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
from app.utils.utils import safe_parse_datetime
from app.v1.endpoints.exceptions import BadRequest
from app.v1.endpoints.functions import set_role
from asyncpg.types import Range
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import Response

from .functions import create_entity, set_commit, update_datastream_last_foi_id

v1 = APIRouter()


user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(None, alias="commit-message")

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
    payload: list = Body(examples=[PAYLOAD_EXAMPLE]),
    commit_message=message,
    current_user=user,
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    async with pgpool.acquire() as conn:
        async with conn.transaction():
            if current_user is not None:
                await set_role(conn, current_user)

            commit_id = await set_commit(
                conn, commit_message, current_user
            )

            for observation_set in payload:
                datastream_id = observation_set.get("Datastream", {}).get(
                    "@iot.id"
                )
                components = observation_set.get("components", [])
                data_array = observation_set.get("dataArray", [])

                if not datastream_id:
                    raise ValueError(
                        "Missing 'datastream_id' in Datastream."
                    )

                # Check that at least phenomenonTime and result are present
                if (
                    "phenomenonTime" not in components
                    or "result" not in components
                ):
                    raise ValueError(
                        "Missing required properties 'phenomenonTime' or 'result' in components."
                    )
                if "featureOfInterest" in components:
                    raise ValueError(
                        "This method does not support 'featureOfInterest' in components. It will support in future."
                    )

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

            if current_user is not None:
    return Response(status_code=status.HTTP_201_CREATED)


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
            if "resultTime" in components:
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
        ph_min_start = None
        ph_max_end = None
        rt_interval = None
        for obs in payload:
            if result_time_idx > -1:
                obs[result_time_idx] = safe_parse_datetime(
                    obs[result_time_idx]
                )
                if obs[result_time_idx] is not None:
                    if rt_interval is None:
                        rt_interval = Range(
                            obs[result_time_idx],
                            obs[result_time_idx],
                            upper_inc=True,
                        )
                    else:
                        rt_interval = Range(
                            min(rt_interval.lower, obs[result_time_idx]),
                            max(rt_interval.upper, obs[result_time_idx]),
                            upper_inc=True,
                        )
            if "/" in obs[ph_idx]:
                ph_time = obs[ph_idx].split("/")
                ph_start = safe_parse_datetime(ph_time[0])
                ph_end = safe_parse_datetime(ph_time[1])
            else:
                ph_start = safe_parse_datetime(obs[ph_idx])
                ph_end = ph_start
            obs[ph_idx : ph_idx + 1] = [ph_start, ph_end]

            if ph_start is not None and (
                ph_min_start is None or ph_start < ph_min_start
            ):
                ph_min_start = ph_start
            if ph_end is not None and (
                ph_max_end is None or ph_end > ph_max_end
            ):
                ph_max_end = ph_end

            default_obs = [result_type, datastream_id, foi_id]

            if (VERSIONING or AUTHORIZATION) and commit_id is not None:
                default_obs.append(commit_id)

            data.append(obs + default_obs)

        observation_types = [
            ot
            for ot in [
                "resultNumber",
                "resultBoolean",
                "resultString",
                "resultJSON",
            ]
            if ot != observation_type
        ]
        cols = observation_types + [
            "phenomenonTimeStart",
            "phenomenonTimeEnd",
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

            ph_components_idx = components.index("phenomenonTime")
            components[ph_components_idx : ph_components_idx + 1] = [
                "phenomenonTimeStart",
                "phenomenonTimeEnd",
            ]

            cols = (
                observation_types
                + components
                + [
                    "resultType",
                    "datastream_id",
                    "featuresofinterest_id",
                ]
            )

        if (VERSIONING or AUTHORIZATION) and commit_id is not None:
            cols.append("commit_id")

        for item in data:
            value = item[0]
            if observation_type == "resultNumber":
                inserts = [None, str(value), None]
            elif observation_type == "resultBoolean":
                inserts = [None, str(value).lower(), None]
            elif observation_type in {"resultString", "resultJSON"}:
                inserts = [None, None, None]

            for val in reversed(inserts):
                item.insert(0, val)

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
            ),
            "resultTime" =
                CASE
                    WHEN $3::timestamptz IS NOT NULL
                    AND $4::timestamptz IS NOT NULL THEN
                        CASE
                            WHEN "resultTime" IS NULL THEN
                                tstzrange($3::timestamptz, $4::timestamptz, '[]')
                            ELSE
                                tstzrange(
                                    LEAST($3::timestamptz, lower("resultTime")),
                                    GREATEST($4::timestamptz, upper("resultTime")),
                                    '[]'
                                )
                        END
                    ELSE "resultTime"
                END
            WHERE id = $5::bigint;
        """
        await conn.execute(
            update_query,
            ph_min_start,
            ph_max_end,
            rt_interval.lower if rt_interval else None,
            rt_interval.upper if rt_interval else None,
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
        query_location_from_thing_datastream = """
            SELECT
                l.id,
                l.name,
                l.description,
                l."encodingType",
                ST_AsGeoJSON(l.location) AS location,
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
                d.id = $1
        """

        result = await conn.fetch(
            query_location_from_thing_datastream, datastream_id
        )

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

                update_query = """
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
            # Empty result has two distinct causes; the old message assumed only
            # the second and misreported the first. Tell them apart.
            datastream_exists = await conn.fetchval(
                'SELECT 1 FROM sensorthings."Datastream" WHERE id = $1::bigint',
                datastream_id,
            )
            if not datastream_exists:
                raise BadRequest(
                    f"Datastream {datastream_id} does not exist."
                )
            raise BadRequest(
                "Cannot auto-generate a FeatureOfInterest: the Thing linked to "
                f"Datastream {datastream_id} has no Location. Provide a "
                "FeatureOfInterest explicitly or add a Location to the Thing."
            )
