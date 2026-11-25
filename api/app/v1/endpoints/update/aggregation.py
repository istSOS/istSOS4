"""Administrator update endpoint for aggregation settings."""

from typing import Annotated

from app.utils.utils import require_json_content_type
from app.v1.endpoints.aggregation_functions import (
    COLUMNS,
    MAX_ID,
    MUTABLE_KEYS,
    TABLE,
    ensure_configuration_available,
    ensure_datastreams_exist,
    fetch_configuration,
    pool_dependency,
    require_administrator,
    serialize_configuration,
    stored_values,
    user,
    validate_allowed_keys,
    validate_patch_payload,
)
from fastapi import APIRouter, Body, Path, Request
from fastapi.responses import JSONResponse

v1 = APIRouter(tags=["Aggregations"])


@v1.patch(
    "/Aggregations({aggregation_id})",
    summary="Update aggregation settings",
)
async def patch_aggregation_configuration(
    request: Request,
    aggregation_id: Annotated[int, Path(ge=1, le=MAX_ID)],
    payload: Annotated[
        dict, Body(examples=[{"minInputQuality": 2, "minCoverage": 0.9}])
    ],
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    require_json_content_type(request)
    validate_allowed_keys(payload, MUTABLE_KEYS)
    async with pool.acquire() as connection, connection.transaction():
        await ensure_configuration_available(connection)
        existing = await fetch_configuration(
            connection, aggregation_id, for_update=True
        )
        values = validate_patch_payload(payload, stored_values(existing))
        await ensure_datastreams_exist(connection, values)
        mutable = [key for key in COLUMNS if key != "datastreamId"]
        assignments = ", ".join(
            f"{COLUMNS[key]}=${i}" for i, key in enumerate(mutable, start=2)
        )
        row = await connection.fetchrow(
            f"UPDATE {TABLE} SET {assignments}, updated_at=clock_timestamp() WHERE id=$1 RETURNING *;",
            aggregation_id,
            *(values[key] for key in mutable),
        )
    return JSONResponse(content=serialize_configuration(row))
