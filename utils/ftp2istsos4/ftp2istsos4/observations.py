import csv
import io
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .errors import DuplicateObservationError


BULK_OBSERVATION_BATCH_SIZE = 5000
QC_NOT_EXECUTED = 0b00
QC_REMAINING = 0b01
QC_PROBLEM = 0b10
QC_OK = 0b11


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


def column_datastream_id(column, client):
    datastream_id = column.get("@iot.id") or column.get("datastream_@iot.id")
    if datastream_id is not None:
        return datastream_id

    datastream_name = column_datastream_name(column)
    if datastream_name:
        return client.resolve_datastream_id(datastream_name)

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


def sensor_things_observations(client, text, file_config, tz_name):
    dt_column = datetime_column(file_config)
    values_config = value_columns(file_config)
    if not values_config:
        return [], 0

    datastream_ids = {}
    for column in values_config:
        key = column.get("idx")
        datastream_id = column_datastream_id(column, client)
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
            if not client.update:
                latest = client.datastream_phenomenon_time_end(datastream_id)
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


def bulk_observations_payload(observations):
    by_datastream = {}
    for observation in observations:
        datastream_id = observation["Datastream"]["@iot.id"]
        phenomenon_time = observation["phenomenonTime"]
        items = by_datastream.setdefault(datastream_id, {})
        if phenomenon_time in items:
            continue
        items[phenomenon_time] = observation

    payload = []
    for datastream_id, items in by_datastream.items():
        payload.append(
            {
                "Datastream": {"@iot.id": datastream_id},
                "components": [
                    "result",
                    "phenomenonTime",
                    "resultTime",
                    "resultQuality",
                ],
                "dataArray": [
                    [
                        item["result"],
                        item["phenomenonTime"],
                        item.get("resultTime") or item["phenomenonTime"],
                        item["resultQuality"],
                    ]
                    for item in items.values()
                ],
            }
        )
    return payload


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


def filter_observations_after_datastream_range(client, observations):
    filtered = []
    skipped = 0
    for observation in observations:
        datastream_id = observation["Datastream"]["@iot.id"]
        latest = client.datastream_phenomenon_time_end(datastream_id)
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


def existing_observations_by_time(client, datastream_id, observations):
    start, end = observation_range(observations)
    if start is None or end is None:
        return {}
    existing = client.fetch_observations(datastream_id, start, end)
    return {
        observation_time_key(item.get("phenomenonTime")): item
        for item in existing
        if item.get("@iot.id") is not None
    }


def filter_observations_missing_from_time_window(client, observations):
    filtered = []
    skipped = 0
    for datastream_id, group in observations_by_datastream(observations).items():
        existing_by_time = existing_observations_by_time(
            client, datastream_id, group
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


def post_observations_individually(client, observations, label):
    posted = 0
    skipped = 0
    for obs_index, observation in enumerate(observations, start=1):
        try:
            response = client.post_observation(observation)
        except DuplicateObservationError:
            skipped += 1
            continue

        posted += 1
        print(
            f"  POST /Observations {label} "
            f"{obs_index}/{len(observations)} -> "
            f"{response.status_code}",
            flush=True,
        )
    return posted, skipped


def patch_observations(client, observations, label):
    updated = 0
    posted = 0
    skipped = 0
    for datastream_id, group in observations_by_datastream(observations).items():
        existing_by_time = existing_observations_by_time(
            client, datastream_id, group
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
            client.patch_observation(existing["@iot.id"], observation)
            updated += 1

        missing_posted, missing_skipped, missing_updated = post_observations(
            client, missing, label, apply_range_filter=False
        )
        posted += missing_posted
        skipped += missing_skipped
        updated += missing_updated

    if updated:
        print(f"  PATCH /Observations {label}: updated {updated}", flush=True)
    if skipped:
        print(f"  skipped unchanged existing observations: {skipped}", flush=True)
    return posted, skipped, updated


def post_observations(client, observations, label, apply_range_filter=True):
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
    if client.update and apply_range_filter:
        return patch_observations(client, observations, label)

    if apply_range_filter:
        if getattr(client, "observation_mode", "append") == "backfill":
            observations, skipped_existing = (
                filter_observations_missing_from_time_window(
                    client, observations
                )
            )
            skipped_message = (
                "skipped existing observations found in parsed time window"
            )
        else:
            observations, skipped_existing = (
                filter_observations_after_datastream_range(client, observations)
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
                response = client.post_bulk_observations(batch)
            except DuplicateObservationError:
                print(
                    f"  POST /BulkObservations {label} "
                    f"batch {batch_index}/{len(batches)} "
                    "contains existing observations; retrying individually",
                    flush=True,
                )
                fallback_posted, fallback_skipped = (
                    post_observations_individually(
                        client,
                        batch,
                        f"{label} batch {batch_index}/{len(batches)}",
                    )
                )
                posted += fallback_posted
                skipped_duplicates += fallback_skipped
                client.fetch_datastreams()
                if fallback_skipped:
                    print(
                        f"  skipped existing observations: "
                        f"{fallback_skipped}",
                        flush=True,
                    )
                continue

            posted += len(batch)
            client.record_observation_times(batch)
            print(
                f"  POST /BulkObservations {label} "
                f"batch {batch_index}/{len(batches)} "
                f"{len(batch)} observations -> {response.status_code}",
                flush=True,
            )
        return posted, skipped_duplicates, updated

    fallback_posted, fallback_skipped = post_observations_individually(
        client, observations, label
    )
    posted += fallback_posted
    skipped_duplicates += fallback_skipped
    client.record_observation_times(observations)
    if fallback_skipped:
        print(
            f"  skipped existing observations: {fallback_skipped}",
            flush=True,
        )
    return posted, skipped_duplicates, updated
