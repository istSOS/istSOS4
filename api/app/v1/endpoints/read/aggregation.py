"""Administrator read endpoints for aggregation settings and processing state."""

import json
from urllib.parse import urlencode

from app.v1.endpoints.aggregation_functions import (
    MAX_ID,
    TABLE,
    base_url,
    ensure_configuration_available,
    fetch_configuration,
    pool_dependency,
    require_administrator,
    serialize_configuration,
    user,
)
from app.v1.endpoints.exceptions import ServiceUnavailable
from fastapi import APIRouter, Path, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

v1 = APIRouter(tags=["Aggregations"])


@v1.get("/Aggregations", summary="List aggregation configurations")
async def list_aggregation_configurations(
    datastream_id: int | None = Query(
        default=None, alias="datastreamId", ge=1, le=MAX_ID
    ),
    enabled: bool | None = Query(default=None),
    top: int = Query(default=100, alias="$top", ge=1, le=1000),
    skip: int = Query(default=0, alias="$skip", ge=0, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection:
        await ensure_configuration_available(connection)
        rows = await connection.fetch(
            f"SELECT * FROM {TABLE} WHERE ($1::bigint IS NULL OR datastream_id=$1) "
            "AND ($2::boolean IS NULL OR enabled=$2) ORDER BY id LIMIT $3 OFFSET $4;",
            datastream_id,
            enabled,
            top + 1,
            skip,
        )
    response = {"value": [serialize_configuration(row) for row in rows[:top]]}
    if len(rows) > top:
        parameters = {"$top": top, "$skip": skip + top}
        if datastream_id is not None:
            parameters["datastreamId"] = datastream_id
        if enabled is not None:
            parameters["enabled"] = str(enabled).lower()
        response["@iot.nextLink"] = (
            f"{base_url()}/Aggregations?{urlencode(parameters)}"
        )
    return JSONResponse(content=response)


@v1.get(
    "/Aggregations({aggregation_id})",
    summary="Get an aggregation configuration",
)
async def get_aggregation_configuration(
    aggregation_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection:
        await ensure_configuration_available(connection)
        row = await fetch_configuration(connection, aggregation_id)
    return JSONResponse(content=serialize_configuration(row))


@v1.get(
    "/Aggregations({aggregation_id})/ProcessingState",
    summary="Read the latest service-managed processing state",
)
async def get_processing_state(
    aggregation_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection:
        await ensure_configuration_available(connection)
        await fetch_configuration(connection, aggregation_id)
        if not await connection.fetchval(
            "SELECT to_regclass('aggregation.processing_state') IS NOT NULL"
        ):
            raise ServiceUnavailable(
                "Processing-state storage is not installed. Apply database/istsos_aggregation.sql."
            )
        row = await connection.fetchrow(
            "SELECT * FROM aggregation.processing_state WHERE aggregation_id=$1",
            aggregation_id,
        )
        if row is None:
            raise ServiceUnavailable(
                "Processing state is missing. Reapply database/istsos_aggregation.sql."
            )
    fields = {
        "aggregationId": "aggregation_id",
        "status": "status",
        "runId": "run_id",
        "lastStartedAt": "last_started_at",
        "lastFinishedAt": "last_finished_at",
        "lastSuccessfulAt": "last_successful_at",
        "lastError": "last_error",
        "consecutiveFailures": "consecutive_failures",
        "checkpoint": "checkpoint",
        "updatedAt": "updated_at",
    }
    result = {key: row[column] for key, column in fields.items()}
    for key in ("lastError", "checkpoint"):
        if isinstance(result[key], str):
            result[key] = json.loads(result[key])
    return JSONResponse(content=jsonable_encoder(result))
