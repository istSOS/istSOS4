import csv
import io
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from istsos4_client import Client, Datastream, Observation, TimeInterval

from .errors import DuplicateObservationError


BULK_OBSERVATION_BATCH_SIZE = 5000
QC_NOT_EXECUTED = 0b00
QC_REMAINING = 0b01
QC_PROBLEM = 0b10
QC_OK = 0b11


@dataclass
class Target:
    """Where one run writes: the istSOS4 client, the import policy of the
    source being processed, and the datastream cache the importers look
    names up in."""

    api: Client
    dry_run: bool = False
    update: bool = False
    observation_mode: str = "append"
    by_name: dict = field(default_factory=dict)
    by_id: dict = field(default_factory=dict)


def load_datastreams(target):
    """Fill the name -> id cache. Also the first authenticated call of a run,
    so bad credentials fail here rather than mid-import."""
    print(f"GET {target.api.base_url}/Datastreams", flush=True)
    by_name = {}
    by_id = {}
    duplicates = set()
    for datastream in target.api.list(Datastream):
        if not datastream.name or datastream.iot_id is None:
            continue
        if datastream.name in by_name:
            duplicates.add(datastream.name)
        by_name[datastream.name] = datastream.iot_id
        by_id[datastream.iot_id] = datastream

    if duplicates:
        names = ", ".join(sorted(duplicates))
        raise RuntimeError(f"Duplicate datastream names in istSOS: {names}")

    target.by_name = by_name
    target.by_id = by_id
    print(f"Datastreams loaded: {len(by_name)}", flush=True)
    return by_name


def resolve_datastream_id(target, name):
    if name not in target.by_name:
        print(
            f"Datastream '{name}' not found. Refreshing datastream list.",
            flush=True,
        )
        load_datastreams(target)
    if name not in target.by_name:
        available = ", ".join(sorted(target.by_name)[:20])
        suffix = ""
        if len(target.by_name) > 20:
            suffix = f", ... ({len(target.by_name)} total)"
        raise ValueError(
            f"Datastream '{name}' does not exist in istSOS. "
            f"Available datastreams: {available}{suffix}"
        )
    return target.by_name[name]


def datastream_phenomenon_time_end(target, datastream_id):
    datastream = target.by_id.get(datastream_id)
    if datastream is None or datastream.phenomenon_time is None:
        return None
    return datastream.phenomenon_time.end


def record_observation_times(target, observations):
    """Extend the cached phenomenonTime ranges with what was just posted,
    so append mode skips these rows without re-reading every datastream."""
    for observation in observations:
        datastream = target.by_id.get(observation["Datastream"]["@iot.id"])
        observed = parse_observation_time(observation.get("phenomenonTime"))
        if datastream is None or observed is None:
            continue
        interval = datastream.phenomenon_time
        if interval is None:
            datastream.phenomenon_time = TimeInterval(
                start=observed, end=observed
            )
        elif observed > interval.end:
            interval.end = observed


def is_duplicate_observation_error(exc):
    """True when istSOS4 rejected a write only because the row already exists."""
    response = exc.response
    if response is None:
        return False
    text = response.text
    return (
        response.status_code == 400
        and "duplicate key value violates unique constraint" in text
        and "unique_observation_phenomenontime_datastreamid" in text
    ) or (response.status_code == 409 and "Observation already exists" in text)


def to_observation(payload):
    """Importer dict -> Observation entity."""
    phenomenon_time = payload["phenomenonTime"]
    return Observation(
        datastream=payload["Datastream"]["@iot.id"],
        phenomenon_time=phenomenon_time,
        result=payload["result"],
        result_time=payload.get("resultTime") or phenomenon_time,
        result_quality=payload.get("resultQuality"),
    )


def dry_run_status(payload, action):
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
    print(f"DRY RUN: {action} disabled", flush=True)
    return 201


def post_observation(target, observation):
    print(f"POST {target.api.base_url}/Observations", flush=True)
    if target.dry_run:
        return dry_run_status(observation, "POST")
    try:
        return target.api.post(to_observation(observation))
    except requests.HTTPError as exc:
        if is_duplicate_observation_error(exc):
            raise DuplicateObservationError(str(exc)) from exc
        raise


def post_bulk_observations(target, observations):
    print(f"POST {target.api.base_url}/BulkObservations", flush=True)
    if target.dry_run:
        return dry_run_status(observations, "POST")
    try:
        target.api.bulk_observations(
            [to_observation(observation) for observation in observations]
        )
    except requests.HTTPError as exc:
        if is_duplicate_observation_error(exc):
            raise DuplicateObservationError(str(exc)) from exc
        raise
    return 201


def patch_observation(target, observation_id, observation):
    print(
        f"PATCH {target.api.base_url}/Observations({observation_id})",
        flush=True,
    )
    payload = {
        "result": observation["result"],
        "resultQuality": observation["resultQuality"],
    }
    if target.dry_run:
        return dry_run_status(payload, "PATCH")
    return target.api.patch(
        Observation(
            iot_id=observation_id,
            result=payload["result"],
            result_quality=payload["resultQuality"],
        )
    )


def configured_columns(file_config):
    columns = file_config.get("columns") or []
    if not isinstance(columns, list):
        raise ValueError(
            f"{file_config.get('filename_suffix')}: columns must be a list"
        )
    return columns


def datetime_column(file_config):
    columns = configured_columns(file_config)
    matches = [
        column
        for column in columns
        if isinstance(column, dict) and column.get("type") == "datetime"
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{file_config.get('filename_suffix')}: exactly one datetime "
            "column is required"
        )
    return matches[0]


def value_columns(file_config):
    columns = configured_columns(file_config)
    return [
        column
        for column in columns
        if (
            isinstance(column, dict)
            and column.get("type") != "datetime"
            and (
                column.get("name") is not None
                or column.get("datastream_name") is not None
                or column.get("@iot.id") is not None
                or column.get("datastream_@iot.id") is not None
            )
        )
    ]


def column_datastream_name(column):
    return column.get("name") or column.get("datastream_name")


def column_datastream_id(column, target):
    datastream_id = column.get("@iot.id") or column.get("datastream_@iot.id")
    if datastream_id is not None:
        return datastream_id

    datastream_name = column_datastream_name(column)
    if datastream_name:
        return resolve_datastream_id(target, datastream_name)

    return None


def result_quality_mask(*states):
    mask = 0
    for index, state in enumerate(states[:4]):
        mask |= state << (index * 2)
    return mask


def raw_data_quality_mask():
    return result_quality_mask(QC_OK)


def no_data_quality_mask():
    return result_quality_mask(QC_NOT_EXECUTED)


def parse_datetime(value, column_config, tz_name):
    date_format = column_config.get("format")
    if date_format:
        try:
            parsed = datetime.strptime(value, date_format)
        except ValueError:
            parsed = datetime.fromisoformat(value)
    else:
        parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(tz_name))
    return parsed.isoformat()


def parse_observation_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def observation_time_key(value):
    parsed = parse_observation_time(value)
    if parsed is None:
        return value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def parse_value(value, column_config):
    if value == "":
        return -999.9, no_data_quality_mask()

    column_type = column_config.get("type")
    if column_type == "float":
        try:
            result = float(value)
        except ValueError:
            return -999.9, no_data_quality_mask()
        if not math.isfinite(result):
            return -999.9, no_data_quality_mask()
        return result, raw_data_quality_mask()
    if column_type == "int":
        try:
            return int(value), raw_data_quality_mask()
        except ValueError:
            return -999.9, no_data_quality_mask()
    if column_type == "string":
        return value, raw_data_quality_mask()
    return value, raw_data_quality_mask()


def sniff_dialect(text):
    try:
        return csv.Sniffer().sniff(text[:4096], delimiters="\t;,")
    except csv.Error:
        return csv.excel


def sensor_things_observations(target, text, file_config, tz_name):
    dt_column = datetime_column(file_config)
    values_config = value_columns(file_config)
    if not values_config:
        return [], 0

    datastream_ids = {}
    for column in values_config:
        key = column.get("idx")
        datastream_id = column_datastream_id(column, target)
        if datastream_id is None:
            continue
        datastream_ids[key] = datastream_id

    observations = []
    skipped_existing = 0
    reader = csv.reader(io.StringIO(text, newline=""), sniff_dialect(text))

    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue

        try:
            dt_value = row[int(dt_column["idx"])].strip()
            phenomenon_time = parse_datetime(dt_value, dt_column, tz_name)
        except (IndexError, KeyError, TypeError, ValueError):
            continue

        for column in values_config:
            try:
                raw_value = row[int(column["idx"])].strip()
                result, result_quality = parse_value(raw_value, column)
            except (IndexError, KeyError, TypeError):
                continue

            datastream_id = datastream_ids.get(column.get("idx"))
            if datastream_id is None:
                continue
            if not target.update:
                latest = datastream_phenomenon_time_end(target, datastream_id)
                observed = parse_observation_time(phenomenon_time)
                if (
                    latest is not None
                    and observed is not None
                    and observed <= latest
                ):
                    skipped_existing += 1
                    continue
            observations.append(
                {
                    "Datastream": {"@iot.id": datastream_id},
                    "phenomenonTime": phenomenon_time,
                    "result": result,
                    "resultQuality": str(result_quality),
                }
            )

    if skipped_existing:
        print(
            "  skipped rows already covered by datastream range while parsing: "
            f"{skipped_existing}",
            flush=True,
        )
    return observations, skipped_existing


def deduplicate_observations(observations):
    seen = set()
    deduplicated = []
    for observation in observations:
        key = (
            observation["Datastream"]["@iot.id"],
            observation["phenomenonTime"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(observation)
    return deduplicated


def filter_observations_after_datastream_range(target, observations):
    filtered = []
    skipped = 0
    for observation in observations:
        datastream_id = observation["Datastream"]["@iot.id"]
        latest = datastream_phenomenon_time_end(target, datastream_id)
        observed = parse_observation_time(observation.get("phenomenonTime"))
        if latest is not None and observed is not None and observed <= latest:
            skipped += 1
            continue
        filtered.append(observation)
    return filtered, skipped


def observations_by_datastream(observations):
    grouped = {}
    for observation in observations:
        datastream_id = observation["Datastream"]["@iot.id"]
        grouped.setdefault(datastream_id, []).append(observation)
    return grouped


def observation_range(observations):
    times = [
        parse_observation_time(observation.get("phenomenonTime"))
        for observation in observations
    ]
    times = [item for item in times if item is not None]
    if not times:
        return None, None
    return min(times).isoformat(), max(times).isoformat()


def same_observation(existing, observation):
    return (
        existing.get("result") == observation.get("result")
        and str(existing.get("resultQuality")) == str(observation.get("resultQuality"))
    )


def existing_observations_by_time(target, datastream_id, observations):
    """What istSOS already stores in the window these observations cover,
    keyed by phenomenonTime so the two can be compared field by field."""
    start, end = observation_range(observations)
    if start is None or end is None:
        return {}

    query = (
        f"Datastream/@iot.id eq {datastream_id} "
        f"and phenomenonTime ge {start} and phenomenonTime le {end}"
    )
    print(
        f"GET {target.api.base_url}/Observations?$filter={query}", flush=True
    )

    existing = {}
    for observation in target.api.iter_list(
        Observation,
        filter=query,
        select="@iot.id,phenomenonTime,result,resultQuality",
    ):
        if observation.iot_id is None:
            continue
        row = observation.serialize()
        existing[observation_time_key(row.get("phenomenonTime"))] = {
            "@iot.id": observation.iot_id,
            "result": row.get("result"),
            "resultQuality": row.get("resultQuality"),
        }
    return existing


def filter_observations_missing_from_time_window(target, observations):
    filtered = []
    skipped = 0
    for datastream_id, group in observations_by_datastream(observations).items():
        existing_by_time = existing_observations_by_time(
            target, datastream_id, group
        )
        for observation in group:
            key = observation_time_key(observation.get("phenomenonTime"))
            if key in existing_by_time:
                skipped += 1
                continue
            filtered.append(observation)
    return filtered, skipped


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def post_observations_individually(target, observations, label):
    posted = 0
    skipped = 0
    for obs_index, observation in enumerate(observations, start=1):
        try:
            status = post_observation(target, observation)
        except DuplicateObservationError:
            skipped += 1
            continue

        posted += 1
        print(
            f"  POST /Observations {label} "
            f"{obs_index}/{len(observations)} -> {status}",
            flush=True,
        )
    return posted, skipped


def patch_observations(target, observations, label):
    updated = 0
    posted = 0
    skipped = 0
    for datastream_id, group in observations_by_datastream(observations).items():
        existing_by_time = existing_observations_by_time(
            target, datastream_id, group
        )
        missing = []
        for observation in group:
            existing = existing_by_time.get(
                observation_time_key(observation.get("phenomenonTime"))
            )
            if existing is None:
                missing.append(observation)
                continue
            if same_observation(existing, observation):
                skipped += 1
                continue
            patch_observation(target, existing["@iot.id"], observation)
            updated += 1

        missing_posted, missing_skipped, missing_updated = post_observations(
            target, missing, label, apply_range_filter=False
        )
        posted += missing_posted
        skipped += missing_skipped
        updated += missing_updated

    if updated:
        print(f"  PATCH /Observations {label}: updated {updated}", flush=True)
    if skipped:
        print(f"  skipped unchanged existing observations: {skipped}", flush=True)
    return posted, skipped, updated


def post_observations(target, observations, label, apply_range_filter=True):
    posted = 0
    skipped_duplicates = 0
    updated = 0
    if not observations:
        return posted, skipped_duplicates, updated

    original_count = len(observations)
    observations = deduplicate_observations(observations)
    duplicate_count = original_count - len(observations)
    if duplicate_count:
        skipped_duplicates += duplicate_count
        print(
            f"  skipped duplicate parsed observations: {duplicate_count}",
            flush=True,
        )
    if target.update and apply_range_filter:
        return patch_observations(target, observations, label)

    if apply_range_filter:
        if target.observation_mode == "backfill":
            observations, skipped_existing = (
                filter_observations_missing_from_time_window(
                    target, observations
                )
            )
            skipped_message = (
                "skipped existing observations found in parsed time window"
            )
        else:
            observations, skipped_existing = (
                filter_observations_after_datastream_range(target, observations)
            )
            skipped_message = (
                "skipped observations already covered by datastream range"
            )
        skipped_duplicates += skipped_existing
        if skipped_existing:
            print(
                f"  {skipped_message}: {skipped_existing}",
                flush=True,
            )
        if not observations:
            return posted, skipped_duplicates, updated

    if len(observations) > 5:
        bulk_observations = observations
        batches = list(chunks(bulk_observations, BULK_OBSERVATION_BATCH_SIZE))
        for batch_index, batch in enumerate(batches, start=1):
            try:
                status = post_bulk_observations(target, batch)
            except DuplicateObservationError:
                print(
                    f"  POST /BulkObservations {label} "
                    f"batch {batch_index}/{len(batches)} "
                    "contains existing observations; retrying individually",
                    flush=True,
                )
                fallback_posted, fallback_skipped = (
                    post_observations_individually(
                        target,
                        batch,
                        f"{label} batch {batch_index}/{len(batches)}",
                    )
                )
                posted += fallback_posted
                skipped_duplicates += fallback_skipped
                load_datastreams(target)
                if fallback_skipped:
                    print(
                        f"  skipped existing observations: "
                        f"{fallback_skipped}",
                        flush=True,
                    )
                continue

            posted += len(batch)
            record_observation_times(target, batch)
            print(
                f"  POST /BulkObservations {label} "
                f"batch {batch_index}/{len(batches)} "
                f"{len(batch)} observations -> {status}",
                flush=True,
            )
        return posted, skipped_duplicates, updated

    fallback_posted, fallback_skipped = post_observations_individually(
        target, observations, label
    )
    posted += fallback_posted
    skipped_duplicates += fallback_skipped
    record_observation_times(target, observations)
    if fallback_skipped:
        print(
            f"  skipped existing observations: {fallback_skipped}",
            flush=True,
        )
    return posted, skipped_duplicates, updated
