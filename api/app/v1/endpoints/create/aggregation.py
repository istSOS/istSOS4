"""Administrator create endpoint for aggregation settings."""

from typing import Annotated

from app.utils.utils import require_json_content_type
from app.v1.endpoints.aggregation_functions import (
    COLUMNS,
    TABLE,
    ensure_configuration_available,
    ensure_datastreams_exist,
    pool_dependency,
    require_administrator,
    serialize_configuration,
    user,
    validate_create_payload,
)
from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

v1 = APIRouter(tags=["Aggregations"])

CREATE_EXAMPLE = {
    "datastreamId": 13,
    "minInputQuality": 3,
    "minCoverage": 0.8,
}


@v1.post(
    "/Aggregations",
    status_code=201,
    summary="Create an aggregation configuration",
)
async def create_aggregation_configuration(
    request: Request,
    payload: Annotated[dict, Body(examples=[CREATE_EXAMPLE])],
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    require_json_content_type(request)
    values = validate_create_payload(payload)
    async with pool.acquire() as connection, connection.transaction():
        await ensure_configuration_available(connection)
        await ensure_datastreams_exist(connection, values)
        columns = ", ".join(COLUMNS.values())
        placeholders = ", ".join(f"${i}" for i in range(1, len(COLUMNS) + 1))
        row = await connection.fetchrow(
            f"INSERT INTO {TABLE} ({columns}) VALUES ({placeholders}) RETURNING *;",
            *(values[key] for key in COLUMNS),
        )
    result = serialize_configuration(row)
    return JSONResponse(
        status_code=201,
        content=result,
        headers={"Location": result["@iot.selfLink"]},
    )
