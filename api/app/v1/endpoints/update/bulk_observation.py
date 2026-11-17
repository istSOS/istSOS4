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


import json

from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import (
    coerce_result,
    get_result_type_and_column,
    safe_parse_datetime,
)
from app.v1.endpoints.exceptions import BadRequest, NotFound
from app.v1.endpoints.functions import (
    get_observation_type,
    set_role,
    update_datastream_time_ranges,
)
from asyncpg.types import Range
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import Response

from .functions import check_id_exists, set_commit

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(None, alias="commit-message")

# Components a bulk PATCH may set, besides "@iot.id".
# ponytail: phenomenonTime is rejected: it is the hypertable partition key, so
# changing it means DELETE + re-INSERT (see relocate_observation).
COMPONENTS = (
    "result",
    "resultTime",
    "resultQuality",
    "validTime",
    "parameters",
)

# SQL type of each Observation column the components set, for unnest.
COLUMN_TYPES = {
    "resultType": "int",
    "resultNumber": "float8",
    "resultBoolean": "boolean",
    "resultString": "text",
    "resultJSON": "jsonb",
    "resultTime": "timestamptz",
    "resultQuality": "jsonb",
    "validTime": "tstzrange",
    "parameters": "jsonb",
}

PAYLOAD_EXAMPLE = [
    {
        "Datastream": {"@iot.id": 1},
        "components": ["@iot.id", "result", "resultQuality"],
        "dataArray": [[1, 1.75, "100"], [2, 1.61, "100"]],
    }
]


@v1.api_route(
    "/BulkObservations",
    methods=["PATCH"],
    tags=["Observations"],
    summary="Update multiple Observations",
    description=(
        "Update many Observations in one request, in the dataArray style of "
        "POST /BulkObservations: one set per Datastream, components names "
        f"\"@iot.id\" and any of {', '.join(COMPONENTS)}, each dataArray row "
        "gives their values. phenomenonTime cannot be updated here. One "
        "commit and one transaction for the whole request: if an @iot.id is "
        "not an Observation of its Datastream, nothing is updated. Send only "
        "rows that change: with versioning, an unchanged row fails the "
        "request."
    ),
    status_code=status.HTTP_200_OK,
)
async def bulk_update_observations(
    payload: list = Body(examples=[PAYLOAD_EXAMPLE]),
    commit_message=message,
    current_user=user,
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    sets = parse_payload(payload)
    if not sets:
        return Response(status_code=status.HTTP_200_OK)

    async with pgpool.acquire() as conn:
        async with conn.transaction():
            if current_user is not None:
                await set_role(conn, current_user)

            commit_id = await set_commit(conn, commit_message, current_user)

            for datastream_id, ids, values in sets:
                # Raising rolls back the sets already updated, and the role.
                if not await check_id_exists(
                    conn, "Datastream", datastream_id
                ):
                    raise NotFound(f"Datastream {datastream_id} not found.")
                missing = await get_missing_observation_ids(
                    conn, datastream_id, ids
                )
                if missing:
                    raise NotFound(
                        f"Observations of Datastream {datastream_id} not "
                        f"found: {missing[:20]}"
                        + (" ..." if len(missing) > 20 else "")
                    )

                observation_type = (
                    await get_observation_type(conn, datastream_id)
                    if "result" in values
                    else None
                )
                columns = convert_values(values, observation_type)
                await update_observations(
                    conn, datastream_id, ids, columns, commit_id=commit_id
                )

                result_times = [t for t in columns.get("resultTime", []) if t]
                if result_times:
                    await update_datastream_time_ranges(
                        conn,
                        datastream_id,
                        None,
                        None,
                        min(result_times),
                        max(result_times),
                    )

            if current_user is not None:
                await conn.execute("RESET ROLE;")

    return Response(status_code=status.HTTP_200_OK)


def parse_payload(payload):
    """Check the body and split each set by component.

    Returns one (datastream_id, ids, {component: values}) per set with rows,
    the values in the order of ids.
    """
    sets, seen = [], set()
    for item in payload:
        if not isinstance(item, dict) or set(item) != {
            "Datastream",
            "components",
            "dataArray",
        }:
            raise BadRequest(
                'Each set needs "Datastream", "components" and "dataArray", '
                "nothing else."
            )
        datastream, components, rows = (
            item["Datastream"],
            item["components"],
            item["dataArray"],
        )

        datastream_id = (
            datastream.get("@iot.id") if isinstance(datastream, dict) else None
        )
        if type(datastream_id) is not int:
            raise BadRequest('Datastream needs an integer "@iot.id".')

        if isinstance(components, list) and "phenomenonTime" in components:
            raise BadRequest(
                "phenomenonTime update not supported, use "
                "PATCH /Observations(id)."
            )
        if (
            not isinstance(components, list)
            or not all(isinstance(c, str) for c in components)
            or len(set(components)) != len(components)
            or "@iot.id" not in components
            or len(components) < 2
            or not set(components) <= {"@iot.id", *COMPONENTS}
        ):
            raise BadRequest(
                'components needs "@iot.id" and at least one of '
                f"{', '.join(COMPONENTS)}, each once."
            )

        id_idx = components.index("@iot.id")
        if not isinstance(rows, list) or not all(
            isinstance(row, list)
            and len(row) == len(components)
            and type(row[id_idx]) is int
            for row in rows
        ):
            raise BadRequest(
                f"Each dataArray row needs {len(components)} values, as in "
                'components, and an integer "@iot.id".'
            )

        ids = [row[id_idx] for row in rows]
        if len(set(ids)) != len(ids) or not seen.isdisjoint(ids):
            raise BadRequest("Each @iot.id may appear only once.")
        seen.update(ids)

        if rows:
            values = {
                component: [row[i] for row in rows]
                for i, component in enumerate(components)
                if component != "@iot.id"
            }
            sets.append((datastream_id, ids, values))
    return sets


def convert_values(values, observation_type):
    """Turn {component: values} into {Observation column: values}, as
    POST /BulkObservations converts them."""
    columns = {}
    for component, component_values in values.items():
        if component == "result":
            for value in component_values:
                result_type, row_values, row_columns = (
                    get_result_type_and_column(
                        coerce_result(value, observation_type)
                    )
                )
                result = dict(zip(row_columns, row_values))
                result["resultType"] = result_type
                if result["resultJSON"] is not None:
                    result["resultJSON"] = json.dumps(result["resultJSON"])
                for column, column_value in result.items():
                    columns.setdefault(column, []).append(column_value)
        elif component in ("resultTime", "validTime"):
            columns[component] = [
                parse_time(component, value) for value in component_values
            ]
        else:
            # As a single PATCH stores it: a string is the JSON text itself,
            # null clears the column.
            columns[component] = [
                (
                    value
                    if value is None or isinstance(value, str)
                    else json.dumps(value)
                )
                for value in component_values
            ]
    return columns


def parse_time(component, value):
    """resultTime is an instant, validTime a "start/end" interval; null
    clears the column."""
    if value is None:
        return None
    if component == "resultTime":
        parsed = safe_parse_datetime(value)
        valid = parsed is not None
    else:
        start, _, end = str(value).partition("/")
        start, end = safe_parse_datetime(start), safe_parse_datetime(end)
        parsed = Range(start, end, upper_inc=True)
        valid = start is not None and end is not None
    if not valid:
        raise BadRequest(f"Invalid {component}: {value!r}")
    return parsed


async def get_missing_observation_ids(conn, datastream_id, ids):
    """Return the ids, sorted, that are no Observation of the Datastream
    (None if all are)."""
    return await conn.fetchval(
        """
            SELECT array_agg(v.id ORDER BY v.id)
            FROM unnest($1::bigint[]) AS v(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM sensorthings."Observation" o
                WHERE o.id = v.id AND o.datastream_id = $2
            )
        """,
        ids,
        datastream_id,
    )


async def update_observations(
    conn, datastream_id, ids, columns, commit_id=None
):
    """Set the columns of the Observations ids of a Datastream in one UPDATE,
    one unnest array per column.

    columns maps keys of COLUMN_TYPES, safe to put in the SQL, to one value
    per id.
    """
    names = list(columns)
    params = [ids, datastream_id, *columns.values()]
    arrays = ", ".join(
        f"${i + 3}::{COLUMN_TYPES[name]}[]" for i, name in enumerate(names)
    )
    sets = [f'"{name}" = v."{name}"' for name in names]
    if commit_id is not None:
        params.append(commit_id)
        sets.append(f"commit_id = ${len(params)}")
    await conn.execute(
        f"""
            UPDATE sensorthings."Observation" o
            SET {", ".join(sets)}
            FROM unnest($1::bigint[], {arrays})
                AS v(id, {", ".join(f'"{name}"' for name in names)})
            WHERE o.id = v.id AND o.datastream_id = $2
        """,
        *params,
    )
