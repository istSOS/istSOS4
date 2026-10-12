"""Observation import, run as: xlsx2istsos.py --observations FILE.

FILE is named <thing>_<YYYYMMDDhhmmss+hhmm>.xlsx: the Thing and the time of
its last observation. Each sheet holds one Datastream of that Thing, named
like the sheet, with the columns time, result, resultQuality.
"""

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from istsos4_client import Datastream, Observation
from istsos4_client.client import MAX_ROWS_PER_BULK

COLUMNS = ("time", "result", "resultQuality")
FILENAME = re.compile(r"(?P<thing>.+)_(?P<end>\d{14}[+-]\d{4})")


def parse_filename(xlsx_path):
    match = FILENAME.fullmatch(Path(xlsx_path).stem)
    if match is None:
        raise ValueError(
            "Expected a file named <thing>_<YYYYMMDDhhmmss+hhmm>.xlsx, "
            f"got {Path(xlsx_path).name}."
        )
    return match["thing"], datetime.strptime(match["end"], "%Y%m%d%H%M%S%z")


def parse_time(value, tz, where):
    """An ISO string or datetime cell as an aware datetime, tz if no offset."""
    try:
        if not isinstance(value, datetime):
            value = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError(f"{where}: invalid time {value!r}.") from None
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz)
    return value


def read_sheet(title, df, end, qc=None):
    """[(time, result, resultQuality), ...], or [] when the sheet is empty,
    runs past the file date or repeats a time. A time without offset gets
    end's. qc, if given, replaces every resultQuality."""
    df = df.dropna(how="all")
    if df.empty:
        print(f"{title}: no observations, skipped.")
        return []
    header = tuple(df.columns[: len(COLUMNS)])
    if header != COLUMNS:
        raise ValueError(
            f"Sheet {title}: expected columns {COLUMNS}, got {header}."
        )

    # Last row first: a sheet past the file date is skipped unparsed.
    where = f"Sheet {title}, row {df.index[-1] + 2}"
    last = parse_time(df["time"].iloc[-1], end.tzinfo, where)
    if last > end:
        print(
            f"WARNING {title}: last observation {last.isoformat()} is after "
            f"the file date {end.isoformat()}, skipped."
        )
        return []

    times = df["time"].dropna()
    duplicated = times[times.duplicated()]
    if not duplicated.empty:
        print(
            f"WARNING {title}: {len(duplicated)} duplicate times, first "
            f"{duplicated.iloc[0]} in row {duplicated.index[0] + 2}, skipped."
        )
        return []

    observations = []
    for row in df.itertuples():
        where = f"Sheet {title}, row {row.Index + 2}"
        if pd.isna(row.result):
            raise ValueError(f"{where}: empty result.")
        time = parse_time(row.time, end.tzinfo, where)
        # resultQuality is jsonb: the API stores a str as JSON text and
        # fails with a 500 on anything else, e.g. an int cell.
        quality = row.resultQuality if qc is None else qc
        quality = None if pd.isna(quality) else json.dumps(quality)
        observations.append((time, row.result, quality))
    return observations


def import_observations(
    client, xlsx_path, commit_message, force=False, qc=None
):
    """Post each sheet to its Datastream. Returns the observations sent."""
    thing, end = parse_filename(xlsx_path)
    thing_name = thing.replace("'", "''")
    # dtype=object: cells as written (no float upcast, no date parsing).
    frames = pd.read_excel(xlsx_path, sheet_name=None, dtype=object)

    # Read and match every sheet first: a bad sheet stops the import before
    # anything is posted.
    sheets, missing = [], []
    for title, df in frames.items():
        rows = read_sheet(title, df, end, qc)
        if not rows:
            continue

        name = title.replace("'", "''")
        datastream = next(
            client.iter_list(
                Datastream,
                filter=f"name eq '{name}' and Thing/name eq '{thing_name}'",
                top=1,
            ),
            None,
        )
        if datastream is None:
            missing.append(title)
            continue

        # Every step between consecutive times must be the samplingFrequency
        # (ISO 8601 duration, e.g. PT10M).
        frequency = (datastream.properties or {}).get("samplingFrequency")
        if frequency is not None:
            times = pd.Series(
                pd.to_datetime([row[0] for row in rows], utc=True)
            )
            steps = times.sort_values().diff().iloc[1:]
            wrong = steps[steps != pd.Timedelta(frequency)]
            if not wrong.empty:
                print(
                    f"WARNING {title}: {len(wrong)} steps are not the "
                    f"samplingFrequency {frequency}, first "
                    f"{wrong.iloc[0]} up to "
                    f"{rows[wrong.index[0]][0].isoformat()}, skipped."
                )
                continue

        sheets.append((datastream, rows))

    if missing:
        raise ValueError(
            f"Thing {thing} has no Datastream named {', '.join(missing)}."
        )

    chunks = []
    for datastream, rows in sheets:
        rows = sorted(rows, key=lambda row: row[0])
        for offset in range(0, len(rows), MAX_ROWS_PER_BULK):
            chunks.append(
                (datastream, rows[offset : offset + MAX_ROWS_PER_BULK])
            )

    run_time = datetime.now().astimezone().isoformat(timespec="seconds")
    sent = 0
    for index, (datastream, rows) in enumerate(chunks, start=1):
        observations = [
            Observation(
                phenomenon_time=time,
                result=result,
                result_quality=quality,
                datastream=datastream.iot_id,
            )
            for time, result, quality in rows
        ]
        message = (
            f"execution time {run_time}, {commit_message} for {Path(xlsx_path).name}, "
            f"sheet {datastream.name} ({index}/{len(chunks)})"
        )
        count = client.bulk_observations(
            observations, commit_message=message, force=force
        )
        print(
            f"{message}, {count} observations imported from "
            f"{rows[0][0].isoformat()} to {rows[-1][0].isoformat()}."
        )
        sent += count
    return sent
