"""Administrator CRUD for aggregation settings; no scheduling or calculation."""

import json
import math
from datetime import UTC, datetime
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Body, Depends, Header, Path, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from app import AUTHORIZATION, HOSTNAME, POSTGRES_PORT_WRITE, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import require_json_content_type
from app.v1.endpoints.exceptions import (
    BadRequest,
    Forbidden,
    NotFound,
    ServiceUnavailable,
)

TABLE = "aggregation.datastream_configuration"
MAX_ID = 9223372036854775807
MAX_INT = 2147483647

# Identifiers interpolated into SQL come only from this constant mapping.
COLUMNS = {
    "datastreamId": "datastream_id",
    "enabled": "enabled",
    "mode": "mode",
    "minInputQuality": "min_input_quality",
    "ignoreQuality": "ignore_quality",
    "minCoverage": "min_coverage",
    "minObservations": "min_observations",
    "maxConsecutiveMissing": "max_consecutive_missing",
    "conversionFactor": "conversion_factor",
    "boundaryMode": "boundary_mode",
    "utcOffsetMinutes": "utc_offset_minutes",
    "bucketsToRecompute": "buckets_to_recompute",
    "startTime": "start_time",
    "endTime": "end_time",
    "resultQualityKey": "result_quality_key",
    "missingValues": "missing_values",
}
DEFAULTS = {name: None for name in COLUMNS}
DEFAULTS.update(enabled=True, mode="sampled", ignoreQuality=False)
MUTABLE_KEYS = COLUMNS.keys() - {"datastreamId"}

v1 = APIRouter(tags=["AggregationConfigurations"])
user = Header(default=None, include_in_schema=False)
if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)
pool_dependency = Depends(get_pool_w if POSTGRES_PORT_WRITE else get_pool)

CREATE_EXAMPLE = {
    "datastreamId": 13,
    "minInputQuality": 3,
    "minCoverage": 0.8,
}


def require_administrator(current_user):
    if AUTHORIZATION and (
        current_user is None or current_user.get("role") != "administrator"
    ):
        raise Forbidden()


def _object(payload, allowed):
    if not isinstance(payload, dict):
        raise BadRequest("Payload must be a JSON object.")
    unknown = payload.keys() - allowed
    if unknown:
        raise BadRequest(
            "Invalid or read-only properties: " + ", ".join(sorted(unknown))
        )


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise BadRequest(
            f"'{name}' must be an integer between {minimum} and {maximum}."
        )
    return value


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BadRequest(f"'{name}' must be a finite number.")
    try:
        result = float(value)
    except (OverflowError, ValueError):
        raise BadRequest(f"'{name}' must be a finite number.") from None
    if not math.isfinite(result):
        raise BadRequest(f"'{name}' must be a finite number.")
    return result


def _datetime(value, name):
    if not isinstance(value, str):
        raise BadRequest(f"'{name}' must be an ISO 8601 datetime with a timezone.")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(UTC)
    except ValueError:
        raise BadRequest(
            f"'{name}' must be an ISO 8601 datetime with a timezone."
        ) from None


def validate_create_payload(payload):
    _object(payload, COLUMNS.keys())
    for key in ("datastreamId", "minInputQuality"):
        if key not in payload or payload[key] is None:
            raise BadRequest(f"'{key}' is required and cannot be null.")
    values = DEFAULTS | payload
    values["datastreamId"] = _integer(values["datastreamId"], "datastreamId", 1, MAX_ID)
    values["minInputQuality"] = _integer(
        values["minInputQuality"], "minInputQuality", 0, 3
    )
    for key in ("enabled", "ignoreQuality"):
        if type(values[key]) is not bool:
            raise BadRequest(f"'{key}' must be a boolean.")
    if values["mode"] not in ("sampled", "event"):
        raise BadRequest("'mode' must be 'sampled' or 'event'.")
    for key in (
        "minObservations",
        "maxConsecutiveMissing",
        "bucketsToRecompute",
        "utcOffsetMinutes",
    ):
        if values[key] is not None:
            minimum, maximum = {
                "bucketsToRecompute": (1, MAX_INT),
                "utcOffsetMinutes": (-840, 840),
            }.get(key, (0, MAX_INT))
            values[key] = _integer(values[key], key, minimum, maximum)
    for key in ("minCoverage", "conversionFactor"):
        if values[key] is not None:
            values[key] = _number(values[key], key)
    if values["minCoverage"] is not None and not 0 <= values["minCoverage"] <= 1:
        raise BadRequest("'minCoverage' must be between 0 and 1.")
    if values["boundaryMode"] not in (None, "right_closed", "left_closed"):
        raise BadRequest(
            "'boundaryMode' must be 'right_closed', 'left_closed', or null."
        )
    if values["mode"] == "event" and any(
        values[key] is not None
        for key in ("minCoverage", "minObservations", "maxConsecutiveMissing")
    ):
        raise BadRequest(
            "Sample completeness settings require mode='sampled'; clear them when switching to 'event'."
        )
    for key in ("startTime", "endTime"):
        if values[key] is not None:
            values[key] = _datetime(values[key], key)
    if (
        values["startTime"] is not None
        and values["endTime"] is not None
        and values["startTime"] >= values["endTime"]
    ):
        raise BadRequest("'startTime' must be before 'endTime'.")
    quality_key = values["resultQualityKey"]
    if quality_key is not None:
        if not isinstance(quality_key, str) or not 1 <= len(quality_key.strip()) <= 255:
            raise BadRequest(
                "'resultQualityKey' must be a non-empty string of at most 255 characters, or null."
            )
        values["resultQualityKey"] = quality_key.strip()
    missing = values["missingValues"]
    if missing is not None:
        if not isinstance(missing, list):
            raise BadRequest(
                "'missingValues' must be an array of finite numbers, or null."
            )
        values["missingValues"] = [_number(item, "missingValues") for item in missing]
    return values


def validate_patch_payload(payload, existing):
    _object(payload, MUTABLE_KEYS)
    if not payload:
        raise BadRequest("PATCH must contain at least one editable property.")
    # Validate cross-field rules against the entire locked, merged record.
    return validate_create_payload(existing | payload)


def _stored_values(row):
    values = {key: row[column] for key, column in COLUMNS.items()}
    for key in ("startTime", "endTime"):
        if values[key] is not None:
            values[key] = values[key].isoformat()
    return values


def _base_url():
    return f"{HOSTNAME.rstrip('/')}{SUBPATH}{VERSION}"


def serialize_configuration(row):
    return jsonable_encoder(
        {
            "id": row["id"],
            **_stored_values(row),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "@iot.selfLink": f"{_base_url()}/AggregationConfigurations({row['id']})",
            "ProcessingState@iot.navigationLink": f"{_base_url()}/AggregationConfigurations({row['id']})/ProcessingState",
            "Datastream@iot.navigationLink": f"{_base_url()}/Datastreams({row['datastream_id']})",
        }
    )


async def ensure_configuration_available(connection):
    if not await connection.fetchval("SELECT to_regclass($1) IS NOT NULL;", TABLE):
        raise ServiceUnavailable(
            "Aggregation configuration storage is not installed. Apply database/aggregation_configuration.sql."
        )


async def _fetch_configuration(connection, configuration_id, *, for_update=False):
    row = await connection.fetchrow(
        f"SELECT * FROM {TABLE} WHERE id=$1" + (" FOR UPDATE" if for_update else ""),
        configuration_id,
    )
    if row is None:
        raise NotFound("Aggregation configuration not found.")
    return row


@v1.get("/AggregationConfigurations", summary="List aggregation configurations")
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
            f"{_base_url()}/AggregationConfigurations?{urlencode(parameters)}"
        )
    return JSONResponse(content=response)


@v1.get(
    "/AggregationConfigurations({configuration_id})",
    summary="Get an aggregation configuration",
)
async def get_aggregation_configuration(
    configuration_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection:
        await ensure_configuration_available(connection)
        row = await _fetch_configuration(connection, configuration_id)
    return JSONResponse(content=serialize_configuration(row))


@v1.post(
    "/AggregationConfigurations",
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
        if not await connection.fetchval(
            'SELECT EXISTS (SELECT 1 FROM sensorthings."Datastream" WHERE id=$1);',
            values["datastreamId"],
        ):
            raise BadRequest("Referenced Datastream does not exist.")
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


@v1.patch(
    "/AggregationConfigurations({configuration_id})",
    summary="Update aggregation settings",
)
async def patch_aggregation_configuration(
    request: Request,
    configuration_id: Annotated[int, Path(ge=1, le=MAX_ID)],
    payload: Annotated[
        dict, Body(examples=[{"minInputQuality": 2, "minCoverage": 0.9}])
    ],
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    require_json_content_type(request)
    _object(payload, MUTABLE_KEYS)
    async with pool.acquire() as connection, connection.transaction():
        await ensure_configuration_available(connection)
        existing = await _fetch_configuration(
            connection, configuration_id, for_update=True
        )
        values = validate_patch_payload(payload, _stored_values(existing))
        mutable = [key for key in COLUMNS if key != "datastreamId"]
        assignments = ", ".join(
            f"{COLUMNS[key]}=${i}" for i, key in enumerate(mutable, start=2)
        )
        row = await connection.fetchrow(
            f"UPDATE {TABLE} SET {assignments}, updated_at=clock_timestamp() WHERE id=$1 RETURNING *;",
            configuration_id,
            *(values[key] for key in mutable),
        )
    return JSONResponse(content=serialize_configuration(row))


@v1.delete(
    "/AggregationConfigurations({configuration_id})",
    status_code=204,
    summary="Delete an aggregation configuration",
)
async def delete_aggregation_configuration(
    configuration_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection, connection.transaction():
        await ensure_configuration_available(connection)
        deleted = await connection.fetchval(
            f"DELETE FROM {TABLE} WHERE id=$1 RETURNING id;",
            configuration_id,
        )
        if deleted is None:
            raise NotFound("Aggregation configuration not found.")
    return Response(status_code=204)


@v1.get(
    "/AggregationConfigurations({configuration_id})/ProcessingState",
    summary="Read the latest service-managed processing state",
)
async def get_processing_state(
    configuration_id: int = Path(ge=1, le=MAX_ID),
    current_user=user,
    pool=pool_dependency,
):
    require_administrator(current_user)
    async with pool.acquire() as connection:
        await ensure_configuration_available(connection)
        await _fetch_configuration(connection, configuration_id)
        if not await connection.fetchval(
            "SELECT to_regclass('aggregation.processing_state') IS NOT NULL"
        ):
            raise ServiceUnavailable(
                "Processing-state storage is not installed. Apply database/aggregation_processing_state.sql."
            )
        row = await connection.fetchrow(
            "SELECT * FROM aggregation.processing_state WHERE configuration_id=$1",
            configuration_id,
        )
        if row is None:
            raise ServiceUnavailable(
                "Processing state is missing. Reapply database/aggregation_processing_state.sql."
            )
    fields = {
        "configurationId": "configuration_id",
        "status": "status",
        "phase": "phase",
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
