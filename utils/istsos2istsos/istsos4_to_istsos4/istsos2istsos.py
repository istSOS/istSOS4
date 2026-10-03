"""Copy observations between two istSOS4 SensorThings API instances."""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator

SHARED_DIR = Path(__file__).resolve().parent.parent
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from client import OBSERVATION_COMPONENTS, IstSOS4Client, max_rows_per_bulk

HERE = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


def load_env(path: Path = HERE / ".env") -> None:
    """Load a small dotenv file without requiring python-dotenv."""
    if not path.is_file():
        return

    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid .env entry on line {line_number}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_names(name: str) -> list[str]:
    return [
        value.strip()
        for value in os.getenv(name, "").split(",")
        if value.strip()
    ]


def datastream_name_mapping() -> dict[str, str]:
    source_names = parse_names("DATASTREAMS_FROM")
    target_names = parse_names("DATASTREAMS_TO")
    if not target_names:
        return {name: name for name in source_names}
    if len(source_names) != len(target_names):
        raise ValueError(
            "DATASTREAMS_FROM and DATASTREAMS_TO must contain the same "
            "number of names"
        )
    if len(source_names) != len(set(source_names)):
        raise ValueError("DATASTREAMS_FROM contains duplicate names")
    return dict(zip(source_names, target_names))


def parse_optional_timestamp(name: str) -> datetime | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"{name} must be an ISO 8601 timestamp: {value}"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def is_nodata(
    result: Any, nodata_value: float, tolerance: float = 1e-9
) -> bool:
    """True if a result equals the no-data sentinel (numeric, tolerant compare)."""
    if result is None:
        return False
    try:
        number = float(result)
    except (TypeError, ValueError):
        return False
    return abs(number - nodata_value) <= tolerance


def chunked(
    iterable: Iterable[dict[str, Any]], size: int
) -> Iterator[list[dict[str, Any]]]:
    """Yield successive lists of at most `size` items from a stream."""
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


def index_datastreams(
    datastreams: list[dict[str, Any]], label: str
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for datastream in datastreams:
        name = datastream.get("name")
        if not name:
            raise ValueError(f"A {label} datastream has no name")
        if name in indexed:
            raise ValueError(f"Duplicate {label} datastream name: {name}")
        indexed[name] = datastream
    return indexed


def copy_datastream_observations(
    source: IstSOS4Client,
    target: IstSOS4Client,
    source_datastream: dict[str, Any],
    target_datastream: dict[str, Any],
    start: str | None,
    end: str | None,
    import_nodata: bool,
    nodata_value: float | None,
    chunk_size: int,
) -> tuple[int, int, int]:
    copied = 0
    skipped_existing = 0
    skipped_nodata = 0
    observations = source.get_observations(
        source_datastream["@iot.id"], start, end
    )
    # Process one insert's worth of observations at a time. For each block we run
    # a single anti-duplicate query over the block's day span, then send what is
    # missing as one bulk request.
    for block in chunked(observations, chunk_size):
        days = [
            phenomenon_time_day(observation["phenomenonTime"])
            for observation in block
        ]
        block_start, _ = utc_day_interval(min(days))
        _, block_end = utc_day_interval(max(days))
        existing_times = set(
            target.get_observation_times(
                target_datastream["@iot.id"], block_start, block_end
            )
        )
        pending = [
            observation
            for observation in block
            if observation["phenomenonTime"] not in existing_times
        ]
        skipped_existing += len(block) - len(pending)

        if not import_nodata and nodata_value is not None:
            to_send = [
                observation
                for observation in pending
                if not is_nodata(observation.get("result"), nodata_value)
            ]
            skipped_nodata += len(pending) - len(to_send)
        else:
            to_send = pending

        if to_send:
            target.post_observations(target_datastream["@iot.id"], to_send)
            copied += len(to_send)
    return copied, skipped_existing, skipped_nodata


def phenomenon_time_day(value: str) -> date:
    """Return the UTC day containing a phenomenon time or interval start."""
    interval_start = value.split("/", 1)[0]
    parsed = datetime.fromisoformat(interval_start.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date()


def utc_day_interval(day: date) -> tuple[str, str]:
    start = datetime.combine(day, datetime.min.time(), timezone.utc)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def run() -> None:
    load_env()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    start_dt = parse_optional_timestamp("TIMESTAMP_START_FROM")
    end_dt = parse_optional_timestamp("TIMESTAMP_END_FROM")
    if start_dt and end_dt and start_dt > end_dt:
        raise ValueError(
            "TIMESTAMP_START_FROM must not be after TIMESTAMP_END_FROM"
        )

    import_nodata = parse_bool(os.getenv("IMPORT_NODATA", "true"))
    nodata_value: float | None = None
    if not import_nodata:
        raw_nodata = os.getenv("NODATA_VALUE", "-999.9").strip()
        try:
            nodata_value = float(raw_nodata)
        except ValueError as exc:
            raise ValueError(
                f"NODATA_VALUE must be a number: {raw_nodata}"
            ) from exc

    source = IstSOS4Client(
        required_env("ISTSOS4_FROM_URL"),
        required_env("ISTSOS4_FROM_USER"),
        required_env("ISTSOS4_FROM_PASSWORD"),
    )
    target = IstSOS4Client(
        required_env("ISTSOS4_TO_URL"),
        required_env("ISTSOS4_TO_USER"),
        required_env("ISTSOS4_TO_PASSWORD"),
    )
    network_from = os.getenv("NETWORK_FROM", "").strip()
    network_to = os.getenv("NETWORK_TO", "").strip()
    datastream_mapping = datastream_name_mapping()
    datastreams_from = set(datastream_mapping)

    source_datastreams = index_datastreams(
        source.get_datastreams(network_from), "source"
    )
    if datastreams_from:
        missing_datastreams = sorted(datastreams_from - set(source_datastreams))
        if missing_datastreams:
            raise ValueError(
                "Source datastreams not found: "
                + ", ".join(missing_datastreams)
            )
        source_datastreams = {
            name: datastream
            for name, datastream in source_datastreams.items()
            if name in datastreams_from
        }
    target_datastreams = index_datastreams(
        target.get_datastreams(network_to), "target"
    )
    if not datastream_mapping:
        datastream_mapping = {name: name for name in source_datastreams}
    missing = sorted(
        (source_name, target_name)
        for source_name, target_name in datastream_mapping.items()
        if target_name not in target_datastreams
    )
    if missing:
        logger.warning(
            "Skipping datastreams not found in target: %s",
            ", ".join(
                f"{source_name} -> {target_name}"
                for source_name, target_name in missing
            ),
        )
        source_datastreams = {
            name: datastream
            for name, datastream in source_datastreams.items()
            if datastream_mapping[name] in target_datastreams
        }

    start = start_dt.isoformat().replace("+00:00", "Z") if start_dt else None
    end = end_dt.isoformat().replace("+00:00", "Z") if end_dt else None
    chunk_size = max_rows_per_bulk(len(OBSERVATION_COMPONENTS))
    total = 0
    total_skipped_existing = 0
    total_skipped_nodata = 0
    interval = f"from {start or 'the beginning'} to {end or 'the end'}"
    logger.info(
        "Copying %d datastreams %s in blocks of up to %d observations",
        len(source_datastreams),
        interval,
        chunk_size,
    )
    if not import_nodata:
        logger.info(
            "Discarding no-data observations equal to %s", nodata_value
        )
    for name, source_datastream in source_datastreams.items():
        target_name = datastream_mapping[name]
        count, skipped_existing, skipped_nodata = copy_datastream_observations(
            source,
            target,
            source_datastream,
            target_datastreams[target_name],
            start,
            end,
            import_nodata,
            nodata_value,
            chunk_size,
        )
        total += count
        total_skipped_existing += skipped_existing
        total_skipped_nodata += skipped_nodata
        label = name if name == target_name else f"{name} -> {target_name}"
        message = (
            f"{label}: copied {count}, skipped {skipped_existing} existing"
        )
        if skipped_nodata:
            message += f", {skipped_nodata} no-data"
        logger.info(message)
    summary = (
        f"Completed: copied {total}, skipped {total_skipped_existing} existing"
    )
    if total_skipped_nodata:
        summary += f", {total_skipped_nodata} no-data"
    logger.info(summary)


if __name__ == "__main__":
    run()
