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
import math

from app import AUTHORIZATION
from app.utils.utils import validate_payload_keys, validate_required_keys
from app.v1.endpoints.exceptions import (
    BadRequest,
    Conflict,
    Forbidden,
    NotFound,
    ServiceUnavailable,
)
from asyncpg.exceptions import DataError

AGGREGATION_PROCEDURES = {
    "10 minutes": "aggregate_datastream_10min",
    "1 hour": "aggregate_datastream_1hour",
    "1 day": "aggregate_datastream_1day",
    "1 year": "aggregate_datastream_1year",
}

PROCEDURE_INTERVALS = {
    procedure: interval
    for interval, procedure in AGGREGATION_PROCEDURES.items()
}

AGGREGATIONS = {
    "SUM",
    "SUM_NULL_AS_ONE",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
}

BOUNDARY_MODES = {"right_closed", "left_closed"}
EMPTY_BUCKET_POLICIES = {"missing", "zero_when_available"}

CREATE_KEYS = [
    "sourceDatastream",
    "targetDatastream",
    "bucketInterval",
    "bucketTimezone",
    "aggregation",
    "conversionFactor",
    "bucketsToRecompute",
    "boundaryMode",
    "resultQualityKey",
    "resultQualityAllowed",
    "resultQualityMin",
    "resultQualityMax",
    "availabilityDatastreams",
    "availabilityMaxAge",
    "emptyBucketPolicy",
    "emptyBucketResultQuality",
    "systemTimeIncremental",
    "systemTimeOverlap",
    "featuresOfInterestId",
    "resultType",
    "scheduleInterval",
    "scheduleDelay",
    "fixedSchedule",
    "scheduleTimezone",
    "enabled",
]

PATCH_KEYS = ["enabled"]

DEFAULTS = {
    "bucketInterval": "10 minutes",
    "bucketTimezone": "Etc/GMT-1",
    "aggregation": "SUM",
    "conversionFactor": 1.0,
    "bucketsToRecompute": 3,
    "boundaryMode": "right_closed",
    "availabilityDatastreams": [],
    "emptyBucketPolicy": "missing",
    "emptyBucketResultQuality": 1.0,
    "systemTimeIncremental": False,
    "systemTimeOverlap": "5 minutes",
    "scheduleDelay": "3 minutes",
    "fixedSchedule": True,
    "enabled": True,
}


def require_administrator(current_user):
    if AUTHORIZATION and (
        current_user is None or current_user.get("role") != "administrator"
    ):
        raise Forbidden()


def _require_string(value, field):
    if not isinstance(value, str) or not value.strip():
        raise BadRequest(f"'{field}' must be a non-empty string.")
    return value.strip()


def _require_boolean(value, field):
    if not isinstance(value, bool):
        raise BadRequest(f"'{field}' must be a boolean.")
    return value


def _require_integer(value, field, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int):
        raise BadRequest(f"'{field}' must be an integer.")
    if minimum is not None and value < minimum:
        raise BadRequest(f"'{field}' must be greater than or equal to {minimum}.")
    if maximum is not None and value > maximum:
        raise BadRequest(f"'{field}' must be less than or equal to {maximum}.")
    return value


def _require_number(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BadRequest(f"'{field}' must be a number.")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise BadRequest(f"'{field}' must be finite.")
    return value


def _optional_number(payload, field):
    value = payload.get(field)
    if value is None:
        return None
    return _require_number(value, field)


def validate_create_payload(payload):
    if not isinstance(payload, dict):
        raise BadRequest("Payload must be a dictionary.")

    validate_payload_keys(payload, CREATE_KEYS)
    validate_required_keys(payload, ["sourceDatastream", "targetDatastream"])

    values = DEFAULTS | payload

    values["sourceDatastream"] = _require_string(
        values["sourceDatastream"], "sourceDatastream"
    )
    values["targetDatastream"] = _require_string(
        values["targetDatastream"], "targetDatastream"
    )

    if values["sourceDatastream"] == values["targetDatastream"]:
        raise BadRequest("Source and target Datastreams must be different.")

    values["bucketInterval"] = _require_string(
        values["bucketInterval"], "bucketInterval"
    )
    if values["bucketInterval"] not in AGGREGATION_PROCEDURES:
        allowed = ", ".join(AGGREGATION_PROCEDURES)
        raise BadRequest(f"Unsupported bucketInterval. Allowed: {allowed}.")

    values["bucketTimezone"] = _require_string(
        values["bucketTimezone"], "bucketTimezone"
    )

    values["aggregation"] = _require_string(
        values["aggregation"], "aggregation"
    ).upper()
    if values["aggregation"] not in AGGREGATIONS:
        allowed = ", ".join(sorted(AGGREGATIONS))
        raise BadRequest(f"Unsupported aggregation. Allowed: {allowed}.")

    values["conversionFactor"] = _require_number(
        values["conversionFactor"], "conversionFactor"
    )
    values["bucketsToRecompute"] = _require_integer(
        values["bucketsToRecompute"],
        "bucketsToRecompute",
        minimum=1,
        maximum=2147483647,
    )

    values["boundaryMode"] = _require_string(
        values["boundaryMode"], "boundaryMode"
    ).lower()
    if values["boundaryMode"] not in BOUNDARY_MODES:
        allowed = ", ".join(sorted(BOUNDARY_MODES))
        raise BadRequest(f"Unsupported boundaryMode. Allowed: {allowed}.")

    quality_key = values.get("resultQualityKey")
    if quality_key is not None:
        values["resultQualityKey"] = _require_string(
            quality_key, "resultQualityKey"
        )

    quality_allowed = values.get("resultQualityAllowed")
    if quality_allowed is not None:
        if not isinstance(quality_allowed, list):
            raise BadRequest("'resultQualityAllowed' must be an array.")
        values["resultQualityAllowed"] = [
            _require_number(item, "resultQualityAllowed")
            for item in quality_allowed
        ]

    values["resultQualityMin"] = _optional_number(
        values, "resultQualityMin"
    )
    values["resultQualityMax"] = _optional_number(
        values, "resultQualityMax"
    )
    if (
        values["resultQualityMin"] is not None
        and values["resultQualityMax"] is not None
        and values["resultQualityMin"] > values["resultQualityMax"]
    ):
        raise BadRequest(
            "'resultQualityMin' cannot be greater than 'resultQualityMax'."
        )

    availability = values["availabilityDatastreams"]
    if availability is None:
        availability = []
    if not isinstance(availability, list):
        raise BadRequest("'availabilityDatastreams' must be an array.")

    normalized_availability = []
    for datastream in availability:
        name = _require_string(datastream, "availabilityDatastreams")
        if name not in normalized_availability:
            normalized_availability.append(name)
    values["availabilityDatastreams"] = normalized_availability

    availability_max_age = values.get("availabilityMaxAge")
    if availability_max_age is not None:
        values["availabilityMaxAge"] = _require_string(
            availability_max_age, "availabilityMaxAge"
        )

    values["emptyBucketPolicy"] = _require_string(
        values["emptyBucketPolicy"], "emptyBucketPolicy"
    ).lower()
    if values["emptyBucketPolicy"] not in EMPTY_BUCKET_POLICIES:
        allowed = ", ".join(sorted(EMPTY_BUCKET_POLICIES))
        raise BadRequest(
            f"Unsupported emptyBucketPolicy. Allowed: {allowed}."
        )

    values["emptyBucketResultQuality"] = _require_number(
        values["emptyBucketResultQuality"], "emptyBucketResultQuality"
    )

    if values["availabilityDatastreams"] and not values.get(
        "availabilityMaxAge"
    ):
        raise BadRequest(
            "'availabilityDatastreams' requires 'availabilityMaxAge'."
        )

    if values["emptyBucketPolicy"] == "zero_when_available" and (
        not values["availabilityDatastreams"]
        or not values.get("availabilityMaxAge")
    ):
        raise BadRequest(
            "emptyBucketPolicy=zero_when_available requires "
            "availabilityDatastreams and availabilityMaxAge."
        )

    values["systemTimeIncremental"] = _require_boolean(
        values["systemTimeIncremental"], "systemTimeIncremental"
    )
    values["systemTimeOverlap"] = _require_string(
        values["systemTimeOverlap"], "systemTimeOverlap"
    )

    features_of_interest_id = values.get("featuresOfInterestId")
    if features_of_interest_id is not None:
        values["featuresOfInterestId"] = _require_integer(
            features_of_interest_id,
            "featuresOfInterestId",
            minimum=1,
            maximum=9223372036854775807,
        )

    result_type = values.get("resultType")
    if result_type is not None:
        values["resultType"] = _require_integer(
            result_type,
            "resultType",
            minimum=-2147483648,
            maximum=2147483647,
        )

    schedule_interval = values.get("scheduleInterval")
    if schedule_interval is None:
        schedule_interval = values["bucketInterval"]
    values["scheduleInterval"] = _require_string(
        schedule_interval, "scheduleInterval"
    )
    values["scheduleDelay"] = _require_string(
        values["scheduleDelay"], "scheduleDelay"
    )
    values["fixedSchedule"] = _require_boolean(
        values["fixedSchedule"], "fixedSchedule"
    )

    schedule_timezone = values.get("scheduleTimezone")
    if schedule_timezone is None:
        schedule_timezone = values["bucketTimezone"]
    values["scheduleTimezone"] = _require_string(
        schedule_timezone, "scheduleTimezone"
    )
    values["enabled"] = _require_boolean(values["enabled"], "enabled")

    return values


def validate_patch_payload(payload):
    if not isinstance(payload, dict):
        raise BadRequest("Payload must be a dictionary.")

    validate_payload_keys(payload, PATCH_KEYS)
    validate_required_keys(payload, ["enabled"])
    return {"enabled": _require_boolean(payload["enabled"], "enabled")}


async def ensure_aggregation_available(connection, procedure=None):
    procedure = procedure or "aggregate_datastream_10min"
    signature = f"sensorthings.{procedure}(integer,jsonb)"

    availability = await connection.fetchrow(
        """
        SELECT
            COALESCE(
                current_setting('custom.versioning', true),
                'false'
            )::boolean AS versioning,
            NOT COALESCE(
                current_setting('custom.duplicates', true),
                'true'
            )::boolean AS unique_names,
            to_regprocedure($1) IS NOT NULL AS procedure_exists;
        """,
        signature,
    )

    if not availability["versioning"] or not availability["unique_names"]:
        raise ServiceUnavailable(
            "Aggregation requires VERSIONING=1 and DUPLICATES=0."
        )
    if not availability["procedure_exists"]:
        raise ServiceUnavailable(
            f"Aggregation procedure sensorthings.{procedure} is not installed."
        )


async def validate_database_options(connection, values):
    try:
        interval_validation = await connection.fetchrow(
            """
            SELECT
                $1::text::interval > interval '0 seconds'
                    AS schedule_interval_valid,
                $2::text::interval >= interval '0 seconds'
                    AS schedule_delay_valid,
                CASE
                    WHEN $3::text IS NULL THEN true
                    ELSE $3::text::interval > interval '0 seconds'
                END AS availability_max_age_valid,
                $4::text::interval >= interval '0 seconds'
                    AS system_time_overlap_valid;
            """,
            values["scheduleInterval"],
            values["scheduleDelay"],
            values.get("availabilityMaxAge"),
            values["systemTimeOverlap"],
        )
    except DataError as error:
        raise BadRequest("One or more interval values are invalid.") from error

    interval_messages = {
        "schedule_interval_valid": "'scheduleInterval' must be positive.",
        "schedule_delay_valid": "'scheduleDelay' must be non-negative.",
        "availability_max_age_valid": "'availabilityMaxAge' must be positive.",
        "system_time_overlap_valid": "'systemTimeOverlap' must be non-negative.",
    }
    for key, message in interval_messages.items():
        if not interval_validation[key]:
            raise BadRequest(message)

    requested_timezones = {
        values["bucketTimezone"],
        values["scheduleTimezone"],
    }
    timezone_rows = await connection.fetch(
        """
        SELECT name
        FROM pg_timezone_names
        WHERE name = ANY($1::text[]);
        """,
        list(requested_timezones),
    )
    found_timezones = {row["name"] for row in timezone_rows}
    missing_timezones = sorted(requested_timezones - found_timezones)
    if missing_timezones:
        raise BadRequest(
            f"Unknown timezone: {', '.join(missing_timezones)}."
        )


async def resolve_datastreams(connection, values):
    requested_names = {
        values["sourceDatastream"],
        values["targetDatastream"],
        *values["availabilityDatastreams"],
    }
    rows = await connection.fetch(
        """
        SELECT "id", "name"
        FROM sensorthings."Datastream"
        WHERE "name" = ANY($1::text[]);
        """,
        list(requested_names),
    )
    datastreams = {
        row["name"]: {"id": row["id"], "name": row["name"]}
        for row in rows
    }

    missing = sorted(requested_names - datastreams.keys())
    if missing:
        raise NotFound(f"Datastreams not found: {', '.join(missing)}.")

    target_name = values["targetDatastream"]
    if target_name in values["availabilityDatastreams"]:
        raise BadRequest(
            "The target Datastream cannot be an availability Datastream."
        )

    return datastreams


async def validate_features_of_interest(connection, features_of_interest_id):
    if features_of_interest_id is None:
        return

    exists = await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM sensorthings."FeaturesOfInterest"
            WHERE "id" = $1
        );
        """,
        features_of_interest_id,
    )
    if not exists:
        raise NotFound(
            f"FeatureOfInterest {features_of_interest_id} not found."
        )


def build_database_config(values, datastreams):
    config = {
        "source_datastream_name": values["sourceDatastream"],
        "target_datastream_name": values["targetDatastream"],
        "aggregation": values["aggregation"],
        "buckets_to_recompute": values["bucketsToRecompute"],
        "bucket_timezone": values["bucketTimezone"],
        "conversion_factor": values["conversionFactor"],
        "boundary_mode": values["boundaryMode"],
        "empty_bucket_policy": values["emptyBucketPolicy"],
        "empty_bucket_result_quality": values[
            "emptyBucketResultQuality"
        ],
        "system_time_incremental": values["systemTimeIncremental"],
        "system_time_overlap": values["systemTimeOverlap"],
        # TimescaleDB does not retain the scheduling delay separately from the
        # initial start. Keeping it in config lets the API reproduce the job
        # settings without changing the aggregation procedure's behaviour.
        "schedule_interval": values["scheduleInterval"],
        "schedule_delay": values["scheduleDelay"],
        "schedule_timezone": values["scheduleTimezone"],
    }

    optional_keys = {
        "resultQualityKey": "result_quality_key",
        "resultQualityAllowed": "result_quality_allowed",
        "resultQualityMin": "result_quality_min",
        "resultQualityMax": "result_quality_max",
        "availabilityMaxAge": "availability_max_age",
        "featuresOfInterestId": "featuresofinterest_id",
        "resultType": "result_type",
    }
    for public_key, database_key in optional_keys.items():
        if values.get(public_key) is not None:
            config[database_key] = values[public_key]

    if values["availabilityDatastreams"]:
        config["availability_datastream_ids"] = [
            datastreams[name]["id"]
            for name in values["availabilityDatastreams"]
        ]

    return config


async def lock_job_identity(
    connection,
    procedure,
    source_datastream_id,
    target_datastream_id,
):
    identity = (
        f"sensorthings.{procedure}:"
        f"{source_datastream_id}:{target_datastream_id}"
    )
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0));",
        identity,
    )


async def find_duplicate_job(
    connection,
    procedure,
    source_datastream,
    target_datastream,
    datastreams,
):
    return await connection.fetchval(
        """
        SELECT job_id
        FROM timescaledb_information.jobs
        WHERE proc_schema = 'sensorthings'
          AND proc_name = $1
          AND (
              config->>'source_datastream_name' = $2
              OR config->>'source_datastream_id' = $3::bigint::text
          )
          AND (
              config->>'target_datastream_name' = $4
              OR config->>'target_datastream_id' = $5::bigint::text
          )
        ORDER BY job_id
        LIMIT 1;
        """,
        procedure,
        source_datastream,
        datastreams[source_datastream]["id"],
        target_datastream,
        datastreams[target_datastream]["id"],
    )


async def create_job(connection, values, config):
    procedure = AGGREGATION_PROCEDURES[values["bucketInterval"]]
    initial_start = await connection.fetchval(
        """
        SELECT
            time_bucket(
                $1::text::interval,
                now() - $2::text::interval,
                timezone => $3::text
            )
            + $1::text::interval
            + $2::text::interval;
        """,
        values["scheduleInterval"],
        values["scheduleDelay"],
        values["scheduleTimezone"],
    )

    return await connection.fetchval(
        """
        SELECT add_job(
            $1::text::regproc,
            $2::text::interval,
            config => $3::text::jsonb,
            initial_start => $4::timestamptz,
            scheduled => $5,
            fixed_schedule => $6,
            timezone => $7
        );
        """,
        f"sensorthings.{procedure}",
        values["scheduleInterval"],
        json.dumps(config),
        initial_start,
        values["enabled"],
        values["fixedSchedule"],
        values["scheduleTimezone"],
    )


def raise_duplicate_job(job_id, values):
    raise Conflict(
        f"Aggregation job {job_id} already exists for "
        f"{values['sourceDatastream']} -> {values['targetDatastream']} "
        f"at {values['bucketInterval']}."
    )


def _decode_config(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(value)


def _config_id(config, key):
    value = config.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _isoformat(value):
    if value is None:
        return None
    if getattr(value, "year", None) == 1:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _datastream_reference(config, prefix, by_id, by_name):
    name = config.get(f"{prefix}_datastream_name")
    datastream_id = _config_id(config, f"{prefix}_datastream_id")

    datastream = by_name.get(name) if name is not None else None
    if datastream is None and datastream_id is not None:
        datastream = by_id.get(datastream_id)

    return {
        "id": datastream["id"] if datastream is not None else datastream_id,
        "name": datastream["name"] if datastream is not None else name,
    }


def _serialize_job(row, config, by_id, by_name):
    availability_ids = config.get("availability_datastream_ids") or []
    availability = []
    for value in availability_ids:
        try:
            datastream_id = int(value)
        except (TypeError, ValueError):
            continue
        datastream = by_id.get(datastream_id)
        availability.append(
            {
                "id": datastream_id,
                "name": (
                    datastream["name"] if datastream is not None else None
                ),
            }
        )

    return {
        "id": row["job_id"],
        "enabled": row["scheduled"],
        "procedure": f"{row['proc_schema']}.{row['proc_name']}",
        "sourceDatastream": _datastream_reference(
            config, "source", by_id, by_name
        ),
        "targetDatastream": _datastream_reference(
            config, "target", by_id, by_name
        ),
        "bucketInterval": PROCEDURE_INTERVALS.get(row["proc_name"]),
        "bucketTimezone": config.get("bucket_timezone", "Etc/GMT-1"),
        "aggregation": config.get("aggregation", "SUM"),
        "conversionFactor": config.get("conversion_factor", 1.0),
        "bucketsToRecompute": config.get("buckets_to_recompute", 3),
        "boundaryMode": config.get("boundary_mode", "right_closed"),
        "resultQualityKey": config.get("result_quality_key"),
        "resultQualityAllowed": config.get("result_quality_allowed"),
        "resultQualityMin": config.get("result_quality_min"),
        "resultQualityMax": config.get("result_quality_max"),
        "availabilityDatastreams": availability,
        "availabilityMaxAge": config.get("availability_max_age"),
        "emptyBucketPolicy": config.get("empty_bucket_policy", "missing"),
        "emptyBucketResultQuality": config.get(
            "empty_bucket_result_quality", 1.0
        ),
        "systemTimeIncremental": config.get(
            "system_time_incremental", False
        ),
        "systemTimeOverlap": config.get("system_time_overlap", "5 minutes"),
        "featuresOfInterestId": config.get("featuresofinterest_id"),
        "resultType": config.get("result_type"),
        "scheduleInterval": config.get(
            "schedule_interval", row["schedule_interval"]
        ),
        "scheduleDelay": config.get("schedule_delay"),
        "fixedSchedule": row["fixed_schedule"],
        "scheduleTimezone": config.get(
            "schedule_timezone",
            config.get("bucket_timezone", "Etc/GMT-1"),
        ),
        "initialStart": _isoformat(row["initial_start"]),
        "nextRunAt": _isoformat(row["next_start"]),
        "lastRunStartedAt": _isoformat(row["last_run_started_at"]),
        "lastSuccessfulFinish": _isoformat(row["last_successful_finish"]),
        "lastRunStatus": row["last_run_status"],
        "jobStatus": row["job_status"],
        "lastRunDuration": row["last_run_duration"],
        "totalRuns": row["total_runs"],
        "totalSuccesses": row["total_successes"],
        "totalFailures": row["total_failures"],
    }


async def fetch_aggregation_jobs(connection, job_id=None):
    rows = await connection.fetch(
        """
        SELECT
            j.job_id,
            j.proc_schema,
            j.proc_name,
            j.schedule_interval::text AS schedule_interval,
            j.scheduled,
            j.fixed_schedule,
            j.initial_start,
            j.config::text AS config,
            COALESCE(js.next_start, j.next_start) AS next_start,
            js.last_run_started_at,
            js.last_successful_finish,
            js.last_run_status::text AS last_run_status,
            js.job_status::text AS job_status,
            js.last_run_duration::text AS last_run_duration,
            js.total_runs,
            js.total_successes,
            js.total_failures
        FROM timescaledb_information.jobs j
        LEFT JOIN timescaledb_information.job_stats js
            ON js.job_id = j.job_id
        WHERE j.proc_schema = 'sensorthings'
          AND j.proc_name = ANY($1::text[])
          AND ($2::integer IS NULL OR j.job_id = $2)
        ORDER BY j.job_id;
        """,
        list(PROCEDURE_INTERVALS),
        job_id,
    )

    configs = [_decode_config(row["config"]) for row in rows]
    datastream_ids = set()
    datastream_names = set()

    for config in configs:
        for prefix in ("source", "target"):
            name = config.get(f"{prefix}_datastream_name")
            datastream_id = _config_id(
                config, f"{prefix}_datastream_id"
            )
            if name is not None:
                datastream_names.add(name)
            if datastream_id is not None:
                datastream_ids.add(datastream_id)

        for value in config.get("availability_datastream_ids") or []:
            try:
                datastream_ids.add(int(value))
            except (TypeError, ValueError):
                continue

    datastream_rows = await connection.fetch(
        """
        SELECT "id", "name"
        FROM sensorthings."Datastream"
        WHERE "id" = ANY($1::bigint[])
           OR "name" = ANY($2::text[]);
        """,
        list(datastream_ids),
        list(datastream_names),
    )
    by_id = {
        row["id"]: {"id": row["id"], "name": row["name"]}
        for row in datastream_rows
    }
    by_name = {
        row["name"]: {"id": row["id"], "name": row["name"]}
        for row in datastream_rows
    }

    return [
        _serialize_job(row, config, by_id, by_name)
        for row, config in zip(rows, configs)
    ]


async def fetch_aggregation_job(connection, job_id):
    jobs = await fetch_aggregation_jobs(connection, job_id)
    if not jobs:
        raise NotFound(f"Aggregation job {job_id} not found.")
    return jobs[0]


async def set_job_enabled(connection, job_id, enabled):
    exists = await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM timescaledb_information.jobs
            WHERE job_id = $1
              AND proc_schema = 'sensorthings'
              AND proc_name = ANY($2::text[])
        );
        """,
        job_id,
        list(PROCEDURE_INTERVALS),
    )
    if not exists:
        raise NotFound(f"Aggregation job {job_id} not found.")

    await connection.execute(
        "SELECT alter_job($1, scheduled => $2);", job_id, enabled
    )
