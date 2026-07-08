"""Copy observations between two istSOS4 SensorThings API instances."""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path
from typing import Any

SHARED_DIR = Path(__file__).resolve().parent.parent
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from client import IstSOS4Client

HERE = Path(__file__).resolve().parent
DEFAULT_BATCH_SIZE = 2000
MAX_BATCH_SIZE = 2000


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
    batch_size: int,
) -> tuple[int, int]:
    copied = 0
    skipped = 0
    observations = source.get_observations(
        source_datastream["@iot.id"], start, end
    )
    daily_groups = groupby(
        observations,
        key=lambda observation: phenomenon_time_day(
            observation["phenomenonTime"]
        ),
    )
    for day, daily_group in daily_groups:
        daily_observations = list(daily_group)
        day_start, day_end = utc_day_interval(day)
        existing_times = set(
            target.get_observation_times(
                target_datastream["@iot.id"], day_start, day_end
            )
        )
        pending = [
            observation
            for observation in daily_observations
            if observation["phenomenonTime"] not in existing_times
        ]
        for offset in range(0, len(pending), batch_size):
            bulk = pending[offset : offset + batch_size]
            target.post_observations(target_datastream["@iot.id"], bulk)
            copied += len(bulk)
        skipped += len(daily_observations) - len(pending)
    return copied, skipped


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
    start_dt = parse_optional_timestamp("TIMESTAMP_START_FROM")
    end_dt = parse_optional_timestamp("TIMESTAMP_END_FROM")
    if start_dt and end_dt and start_dt > end_dt:
        raise ValueError(
            "TIMESTAMP_START_FROM must not be after TIMESTAMP_END_FROM"
        )

    batch_size = int(os.getenv("BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    if batch_size <= 0:
        raise ValueError("BATCH_SIZE must be greater than zero")
    if batch_size > MAX_BATCH_SIZE:
        raise ValueError(
            f"BATCH_SIZE must not exceed {MAX_BATCH_SIZE}; larger batches "
            "exceed the asyncpg bind-parameter limit"
        )

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
    procedures_from = {
        name.strip()
        for name in os.getenv("PROCEDURES_FROM", "").split(",")
        if name.strip()
    }

    source_datastreams = index_datastreams(
        source.get_datastreams(network_from), "source"
    )
    if procedures_from:
        missing_procedures = sorted(procedures_from - set(source_datastreams))
        if missing_procedures:
            raise ValueError(
                "Source datastreams not found: "
                + ", ".join(missing_procedures)
            )
        source_datastreams = {
            name: datastream
            for name, datastream in source_datastreams.items()
            if name in procedures_from
        }
    target_datastreams = index_datastreams(
        target.get_datastreams(network_to), "target"
    )
    missing = sorted(set(source_datastreams) - set(target_datastreams))
    if missing:
        print(
            "Skipping datastreams not found in target: " + ", ".join(missing)
        )
        source_datastreams = {
            name: datastream
            for name, datastream in source_datastreams.items()
            if name in target_datastreams
        }

    start = start_dt.isoformat().replace("+00:00", "Z") if start_dt else None
    end = end_dt.isoformat().replace("+00:00", "Z") if end_dt else None
    total = 0
    total_skipped = 0
    interval = f"from {start or 'the beginning'} to {end or 'the end'}"
    print(f"Copying {len(source_datastreams)} datastreams {interval}")
    for name, source_datastream in source_datastreams.items():
        count, skipped = copy_datastream_observations(
            source,
            target,
            source_datastream,
            target_datastreams[name],
            start,
            end,
            batch_size,
        )
        total += count
        total_skipped += skipped
        print(
            f"{name}: copied {count}, skipped {skipped} existing observations"
        )
    print(
        f"Completed: copied {total}, skipped {total_skipped} existing observations"
    )


if __name__ == "__main__":
    run()
