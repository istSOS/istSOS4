"""Import configured istSOS2 observations into existing istSOS4 datastreams."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import yaml

SHARED_DIR = Path(__file__).resolve().parent.parent
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from client import IstSOS2Client, IstSOS4Client, parse_result_value

HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = HERE / "config.yml"

logger = logging.getLogger(__name__)


def load_env(path: Path = HERE / ".env") -> None:
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


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"Config file is not accessible: {path}")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not config:
        raise ValueError(f"Config file is empty: {path}")
    if "istsos" not in config:
        raise ValueError(f"Missing 'istsos' section in {path}")
    return config["istsos"]


def parse_istsos2_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid istSOS2 timestamp: {value}") from exc


def format_istsos2_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iter_time_windows(
    start: datetime,
    end: datetime,
    step: timedelta,
) -> Iterator[tuple[datetime, datetime]]:
    current = start
    while current < end:
        window_end = min(current + step, end)
        yield current, window_end
        current = window_end


def validate_import_job(job: dict[str, Any]) -> None:
    name = job.get("name", "unnamed_job")
    required = {
        "service",
        "procedures_istsos2",
        "procedures_istsos4",
        "step_days",
    }
    missing = sorted(required - set(job))
    if missing:
        raise ValueError(f"Missing keys in job '{name}': {missing}")

    source_procedures = job["procedures_istsos2"]
    target_procedures = job["procedures_istsos4"]
    if not isinstance(source_procedures, list):
        raise ValueError(
            f"'procedures_istsos2' must be a list in job '{name}'"
        )
    if not isinstance(target_procedures, list):
        raise ValueError(
            f"'procedures_istsos4' must be a list in job '{name}'"
        )
    if len(source_procedures) != len(target_procedures):
        raise ValueError(
            f"Config mismatch in job '{name}': procedures_istsos2 has "
            f"{len(source_procedures)} items and procedures_istsos4 has "
            f"{len(target_procedures)}"
        )
    try:
        step_days = float(job["step_days"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"'step_days' must be a number in job '{name}'"
        ) from exc
    if step_days <= 0:
        raise ValueError(f"'step_days' must be greater than 0 in job '{name}'")


def procedure_metadata(
    source: IstSOS2Client,
    service: str,
    procedure: str,
) -> tuple[datetime, datetime, str]:
    details = source.get_procedure(service, procedure)
    outputs = details.get("outputs", [])
    time_output = next(
        (output for output in outputs if output.get("name") == "Time"),
        {},
    )
    interval = time_output.get("constraint", {}).get("interval", [])
    if len(interval) < 2 or not interval[0] or not interval[1]:
        raise ValueError(
            f"Missing time interval for istSOS2 procedure: {procedure}"
        )

    observed_property = next(
        (output for output in outputs if output.get("name") != "Time"),
        None,
    )
    if not observed_property or not observed_property.get("definition"):
        raise ValueError(
            f"Missing observed property for istSOS2 procedure: {procedure}"
        )
    return (
        parse_istsos2_datetime(interval[0]),
        parse_istsos2_datetime(interval[1]),
        observed_property["definition"],
    )


def build_data_array(values: list[list[Any]]) -> list[list[Any]]:
    data_array = []
    for observation in values:
        if len(observation) < 3:
            raise ValueError(f"Invalid istSOS2 observation row: {observation}")
        data_array.append(
            [
                parse_result_value(observation[1]),
                observation[0],
                observation[0],
                str(observation[2]),
            ]
        )
    return data_array


def import_procedure(
    source: IstSOS2Client,
    target: IstSOS4Client,
    service: str,
    source_procedure: str,
    target_procedure: str,
    step: timedelta,
    nodata_value: float | None,
) -> tuple[int, int]:
    target_id = target.get_datastream_id(target_procedure)
    start, end, observed_property = procedure_metadata(
        source,
        service,
        source_procedure,
    )
    inserted_total = 0
    skipped_nodata_total = 0
    for window_start, window_end in iter_time_windows(start, end, step):
        start_text = format_istsos2_datetime(window_start)
        end_text = format_istsos2_datetime(window_end)
        values = source.get_observation_values(
            service,
            source_procedure,
            observed_property,
            start_text,
            end_text,
        )
        data_array = build_data_array(values)
        if nodata_value is not None:
            kept = [
                row
                for row in data_array
                if not is_nodata(row[0], nodata_value)
            ]
            skipped_nodata_total += len(data_array) - len(kept)
            data_array = kept
        inserted = target.post_data_array(target_id, data_array)
        inserted_total += inserted
        logger.info(
            "%s -> %s: inserted %d observations from %s to %s",
            source_procedure,
            target_procedure,
            inserted,
            start_text,
            end_text,
        )
    return inserted_total, skipped_nodata_total


def run_job(
    job: dict[str, Any],
    source: IstSOS2Client,
    target: IstSOS4Client,
    nodata_value: float | None,
) -> tuple[int, int]:
    validate_import_job(job)
    service = job["service"]
    step = timedelta(days=float(job["step_days"]))
    inserted = 0
    skipped_nodata = 0
    for source_procedure, target_procedure in zip(
        job["procedures_istsos2"],
        job["procedures_istsos4"],
    ):
        proc_inserted, proc_skipped_nodata = import_procedure(
            source,
            target,
            service,
            source_procedure,
            target_procedure,
            step,
            nodata_value,
        )
        inserted += proc_inserted
        skipped_nodata += proc_skipped_nodata
    return inserted, skipped_nodata


def run_all_imports() -> None:
    load_env()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config = load_config()
    jobs = config.get("imports", [])
    if not jobs:
        raise ValueError(f"No imports configured in {DEFAULT_CONFIG_PATH}")

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

    source = IstSOS2Client(
        required_env("ISTSOS2_URL"),
        required_env("ISTSOS2_USER"),
        required_env("ISTSOS2_PASSWORD"),
    )
    target = IstSOS4Client(
        required_env("ISTSOS4_URL"),
        required_env("ISTSOS4_USER"),
        required_env("ISTSOS4_PASSWORD"),
    )
    continue_on_error = bool(config.get("continue_on_error", False))
    completed = failed = skipped = inserted_total = 0
    skipped_nodata_total = 0

    if nodata_value is not None:
        logger.info(
            "Discarding no-data observations equal to %s", nodata_value
        )

    for job in jobs:
        name = job.get("name", "unnamed_job")
        if not job.get("enabled", False):
            skipped += 1
            logger.info("Skipping disabled job '%s'", name)
            continue
        try:
            logger.info("Starting job '%s'", name)
            job_inserted, job_skipped_nodata = run_job(
                job, source, target, nodata_value
            )
            inserted_total += job_inserted
            skipped_nodata_total += job_skipped_nodata
            completed += 1
            logger.info("Completed job '%s'", name)
        except Exception as exc:
            failed += 1
            logger.error("job '%s' failed: %s", name, exc)
            if not continue_on_error:
                raise

    summary = (
        f"Completed: jobs={len(jobs)}, completed={completed}, failed={failed}, "
        f"skipped={skipped}, observations={inserted_total}"
    )
    if skipped_nodata_total:
        summary += f", no-data discarded={skipped_nodata_total}"
    logger.info(summary)


if __name__ == "__main__":
    run_all_imports()
