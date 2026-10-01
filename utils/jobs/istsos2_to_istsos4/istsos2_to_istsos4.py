"""Import configured istSOS2 observations into existing istSOS4 datastreams."""

from __future__ import annotations

import logging
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import yaml
from istsos4_client import Client, Datastream, Observation

from istsos2_client import IstSOS2Client, parse_result_value

HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = HERE / "config.yaml"

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


def parse_iso_duration(value: str) -> pd.Timedelta:
    """Parse ISO 8601 durations like PT10M, PT1H, P1D (case-insensitive)."""
    try:
        duration = pd.Timedelta(value.strip().upper())
    except ValueError as exc:
        raise ValueError(f"Invalid ISO 8601 duration: {value}") from exc
    if duration <= pd.Timedelta(0):
        raise ValueError(f"Duration must be greater than 0: {value}")
    return duration


def aggregate_data_array(
    data_array: list[list[Any]],
    window: pd.Timedelta,
) -> list[list[Any]]:
    """Average observations per epoch-aligned window (start, end].

    phenomenonTime/resultTime of each aggregate is the window end.
    """
    if not data_array:
        return []

    df = pd.DataFrame(
        [(row[0], row[1]) for row in data_array], columns=["result", "time"]
    )
    df["value"] = pd.to_numeric(df["result"], errors="coerce")
    df = df.dropna(subset=["value"])
    if df.empty:
        return []

    series = pd.Series(
        df["value"].to_numpy(),
        index=pd.DatetimeIndex(df["time"].map(parse_istsos2_datetime)),
    )
    agg = (
        series.resample(window, closed="right", label="right", origin="epoch")
        .mean()
        .dropna()  # resample emits empty bins between data; drop them
    )

    rows = []
    for ts, value in agg.items():
        ts_str = format_istsos2_datetime(ts.to_pydatetime())
        rows.append([value, ts_str, ts_str, "100"])
    return rows


def coerce_config_datetime(value: Any, field: str) -> datetime | None:
    """YAML only auto-types well-formed timestamps; normalize everything else.

    Without this a typo like ``13:0:00`` stays a str and blows up later as an
    unhelpful str/datetime comparison.
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = parse_istsos2_datetime(value)
    if not isinstance(value, datetime):
        raise ValueError(f"'{field}' must be an ISO 8601 timestamp: {value!r}")
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


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
    observed_property_name: str,
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
        (
            output
            for output in outputs
            if output.get("name") == observed_property_name
        ),
        None,
    )
    if not observed_property or not observed_property.get("definition"):
        raise ValueError(
            f"Observed property '{observed_property_name}' not found for "
            f"istSOS2 procedure: {procedure}"
        )
    return (
        parse_istsos2_datetime(interval[0]),
        parse_istsos2_datetime(interval[1]),
        observed_property["definition"],
    )


def escape_odata_string(value: str) -> str:
    return value.replace("'", "''")


def datastream_id(target: Client, name: str) -> int:
    matches = target.list(
        Datastream, filter=f"name eq '{escape_odata_string(name)}'"
    )
    if not matches:
        raise ValueError(f"Datastream not found in istSOS4: {name}")
    if len(matches) > 1:
        raise ValueError(f"Multiple istSOS4 datastreams are named: {name}")
    return matches[0].iot_id


def observation_id(
    target: Client, ds_id: int, phenomenon_time: str
) -> int | None:
    matches = target.list(
        Observation,
        filter=(
            f"Datastream/@iot.id eq {ds_id} "
            f"and phenomenonTime eq {phenomenon_time}"
        ),
        select="@iot.id",
        top=1,
    )
    return matches[0].iot_id if matches else None


def post_data_array(
    target: Client, ds_id: int, data_array: list[list[Any]]
) -> int:
    """Bulk insert rows of [result, phenomenonTime, resultTime, resultQuality]."""
    if not data_array:
        return 0
    return target.bulk_observations(
        [
            Observation(
                datastream=ds_id,
                result=result,
                phenomenon_time=phenomenon_time,
                result_time=result_time,
                result_quality=quality,
            )
            for result, phenomenon_time, result_time, quality in data_array
        ]
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
    target: Client,
    service: str,
    source_procedure: str,
    target_procedure: str,
    step: timedelta,
    nodata_value: float | None,
    window_start: datetime | None,
    window_end: datetime | None,
    aggregate_window: timedelta | None,
    override: bool | None,
) -> tuple[int, int]:
    ds_id = datastream_id(target, target_procedure)
    procedure, observed_property_name = source_procedure.split(".", 1)
    start, end, observed_property = procedure_metadata(
        source,
        service,
        procedure,
        observed_property_name,
    )
    # Clamp the procedure's data interval to the configured time window.
    if window_start is not None:
        start = max(start, window_start)
    if window_end is not None:
        end = min(end, window_end)
    if start >= end:
        logger.info(
            "%s -> %s: no overlap between procedure interval and "
            "configured time window, skipping",
            source_procedure,
            target_procedure,
        )
        return 0, 0
    inserted_total = 0
    skipped_nodata_total = 0
    for window_start, window_end in iter_time_windows(start, end, step):
        start_text = format_istsos2_datetime(window_start)
        end_text = format_istsos2_datetime(window_end)
        values = source.get_observation_values(
            service,
            procedure,
            observed_property,
            start_text,
            end_text,
        )
        # Normalize, drop no-data, and aggregate identically for both paths so
        # override patches the same phenomenonTime a bulk insert would create.
        # Row layout: [result, phenomenonTime, resultTime, resultQuality].
        data_array = build_data_array(values)
        if nodata_value is not None:
            kept = [
                row
                for row in data_array
                if not is_nodata(row[0], nodata_value)
            ]
            skipped_nodata_total += len(data_array) - len(kept)
            data_array = kept
        if aggregate_window is not None:
            data_array = aggregate_data_array(data_array, aggregate_window)
        if override:
            # Patch already-existing istSOS4 observations in place instead of
            # bulk inserting.
            # ponytail: one lookup per row; batch via a window fetch of
            # (@iot.id, phenomenonTime) if this is ever too slow.
            inserted = 0
            for result, phenomenon_time, _, quality in data_array:
                obs_id = observation_id(target, ds_id, phenomenon_time)
                if obs_id is None:
                    logger.warning(
                        "%s -> %s: no existing observation at %s to patch",
                        source_procedure,
                        target_procedure,
                        phenomenon_time,
                    )
                    continue
                target.patch(
                    Observation(
                        iot_id=obs_id, result=result, result_quality=quality
                    )
                )
                inserted += 1
            inserted_total += inserted
        else:
            inserted = post_data_array(target, ds_id, data_array)
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
    target: Client,
    nodata_value: float | None,
) -> tuple[int, int]:
    validate_import_job(job)
    service = job["service"]
    step = timedelta(days=float(job["step_days"]))
    override = job.get("override")
    window_start = coerce_config_datetime(job.get("start_date"), "start_date")
    window_end = coerce_config_datetime(job.get("end_date"), "end_date")
    aggregate = job.get("aggregate")
    aggregate_window = None
    if aggregate is not None and str(aggregate).strip().lower() != "none":
        aggregate_window = parse_iso_duration(str(aggregate))
        logger.info(
            "Aggregating observations over %s windows (start, end], "
            "phenomenonTime = window end",
            aggregate_window,
        )
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
            window_start,
            window_end,
            aggregate_window,
            override,
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
    target = Client(
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
