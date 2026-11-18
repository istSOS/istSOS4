"""Historical QC: recompute resultQuality exactly as the Flink QC job does
(flink/service/job/istsos4/kafkaQC.py and utils/IstsosQC.py), reading the
observations from istSOS4 instead of Kafka.

Flink checks each new observation as the newest of a buffer holding the
previous N-1 observations of its datastream: one SaQC call per constraint and
observation (~10 ms). Here the buffers are laid end to end on a made-up time
axis, far apart, so one SaQC call checks thousands of them.
"""

import logging
import math
import os
import re
import sys
import time
from itertools import islice

import isodate
import numpy as np
import pandas as pd
import requests
from istsos4_client import Client, Datastream, Observation
from istsos4_client.client import MAX_ROWS_PER_BULK
from saqc import SaQC

NO_DATA = 999.9
NO_DATA_VALUES = [NO_DATA, -999, -9999, -999.0, -999.9, -999.99]
MIN_COVERAGE = 0.8  # IstsosQC.py
WINDOW = re.compile(r"^\s*(\d+)\s*(s|min|m|h|d)\s*$", re.IGNORECASE)  # kafkaQC
BAD = 255.0  # SaQC "float" scheme

PAGE = 10_000  # observations per request
ROWS_PER_CALL = 2_000_000  # stacked buffer rows per SaQC call
EPOCH = np.datetime64("1970-01-01", "ns")  # made-up time axis, whose
AXIS_LENGTH = np.timedelta64(250 * 365, "D")  # offsets fit in int64 ns
SAMPLE = 50  # buffers per constraint also run on their own
TWO_VALUES = (  # bad arguments fail on any data, e.g. these
    np.array(["2000-01-01T00", "2000-01-01T01"], "datetime64[ns]"),
    np.array([1.0, 2.0]),
)

# Log format and colours as in utils/services/mqtt2istsos.
LOGGER = logging.getLogger("qc_historical")
SUCCESS, NOTICE = 35, 60
logging.addLevelName(SUCCESS, "SUCCESS")
logging.addLevelName(NOTICE, "NOTICE")
COLORS = {logging.WARNING: "\033[33m", logging.ERROR: "\033[31m",
          logging.CRITICAL: "\033[31m", SUCCESS: "\033[32m"}


class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = COLORS.get(record.levelno, "\033[37m")
        return f"{color}{super().format(record)}\033[0m"


def configure_logging(level_name):
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColorFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    root.addHandler(handler)


def run(c, times, values):
    """IstsosQC's SaQC call for constraint c: which values it flags BAD."""
    index = pd.DatetimeIndex(times).tz_localize("UTC")
    data = pd.DataFrame(
        {"result": values, "resultQuality": 0, "phenomenonTime": index},
        index=index,
    )
    args = {
        k: v
        for k, v in c.items()
        if k not in ("type", "column") and v is not None
    }
    qc = getattr(SaQC(data, scheme="float"), c["type"])(
        c.get("column", "result"), **args
    )
    return qc.flags["result"].to_numpy() == BAD


def newest_bad(name, c, times, values, targets, n):
    """For each target as the newest of its buffer (the n-1 values before it
    plus itself): whether SaQC flags it BAD, and whether the call ran. As in
    Flink, a call that fails leaves only that observation's slot 00.

    Buffers go end to end, a gap apart, so no window reaches the next one. A
    sample of buffers also runs on its own: if it disagrees (the check looks
    at all the data, or counts values instead of time), every buffer runs on
    its own, as in Flink. So do the buffers of a stacked call that fails."""
    first = np.maximum(targets - n + 1, 0)

    def alone(k):  # Flink: one SaQC call on the buffer, None if it fails
        own = slice(first[k], targets[k] + 1)
        try:
            return run(c, times[own], values[own])
        except Exception:
            return None

    def each(ks):  # (bad, ran) of these buffers, each run on its own
        flags = [alone(k) for k in ks]
        ran = np.array([f is not None for f in flags], bool)
        return np.array([f is not None and f[-1] for f in flags], bool), ran

    texts = [v for v in c.values() if isinstance(v, str)] + ["0s"]
    span = pd.to_timedelta(texts, errors="coerce").max().to_timedelta64()
    gap = np.timedelta64(1, "D") + 2 * span
    per_call = max(1, min(ROWS_PER_CALL, AXIS_LENGTH // gap) // n)
    spread = np.linspace(0, len(targets) - 1, SAMPLE).astype(int)
    sample = set(spread) | set(range(min(n, len(targets))))

    bad, ran = np.zeros(len(targets), bool), np.ones(len(targets), bool)
    for start in range(0, len(targets), per_call):
        part = slice(start, start + per_call)
        rows = targets[part, None] - np.arange(n - 1, -1, -1)
        inside = rows >= 0
        buffer, rows = np.nonzero(inside)[0], rows[inside]
        step = np.minimum(np.diff(times[rows]), gap)  # capped real steps
        step[np.diff(buffer) != 0] = gap
        axis = EPOCH + np.concatenate(
            [[np.timedelta64(0, "ns")], np.cumsum(step)]
        )
        newest = np.cumsum(inside.sum(axis=1)) - 1
        try:
            flags = run(c, axis, values[rows])
        except Exception as error:  # some buffer breaks it
            LOGGER.debug("%s: %s stacked call failed (%s), running its "
                         "buffers one at a time", name, c["type"],
                         str(error).partition("\n")[0])
            bad[part], ran[part] = each(range(start, start + len(newest)))
            continue
        bad[part] = flags[newest]

        for k in sample & set(range(start, start + len(newest))):
            single, own = alone(k), times[first[k] : targets[k] + 1]
            end = newest[k - start] + 1
            stacked = flags[end - len(own) : end]
            far = own >= own[0] + span  # past SaQC start-up effects
            if single is None or (single[far] != stacked[far]).any():
                LOGGER.warning("%s: %s gives different results when stacked, "
                               "running %d buffers one at a time (slow)",
                               name, c["type"], len(targets))
                return each(range(len(targets)))
    return bad, ran


def quality(name, constraints, times, values, targets, n, sampling):
    """IstsosQC.run for each target as the newest of its buffer: 2-bit slots,
    slot 0 no data, slots 1-4 the constraints (11 passed, 10 failed, 00 not
    run)."""
    q = np.where(np.isnan(values[targets]), 0, 0b11)
    series = pd.Series(values, index=times)
    for slot, c in enumerate(constraints[:4], start=1):
        try:  # bad arguments: Flink leaves the slot 00 everywhere too
            run(c, *TWO_VALUES)
            valid = np.ones(len(targets), bool)
            if isinstance(window := c.get("window"), str):
                # IstsosQC.valid_rows: a full window of history, enough values
                w = pd.Timedelta(window)
                required = c.get("min_periods") or (
                    math.ceil(MIN_COVERAGE * (w / sampling)) if sampling else 1
                )
                count = series.rolling(window).count().to_numpy()[targets]
                history = (
                    times[targets] - times[np.maximum(targets - n + 1, 0)]
                )
                valid = (count >= required) & (history >= w)
        except Exception as error:
            reason = str(error).partition("\n")[0]
            LOGGER.warning("%s: slot %d %s not run: %s", name, slot, c, reason)
            continue
        started = time.monotonic()
        bad, ran = newest_bad(name, c, times, values, targets, n)
        valid &= ran
        checked = valid & (q != 0)  # no data (0) stays 0
        shift = 2 * slot
        new = (q & ~(3 << shift)) | (np.where(bad, 2, 3) << shift)
        q = np.where(checked, new, q)
        LOGGER.info(
            "%s: slot %d %s: %d passed, %d failed, %d no data, "
            "%d not run (window not full or SaQC error) (%.1fs)",
            name, slot, c["type"], (checked & ~bad).sum(),
            (checked & bad).sum(), (q == 0).sum(), (~valid & (q != 0)).sum(),
            time.monotonic() - started,
        )
    return q


def fill_missing(writer, datastream, before, period, start, end, sampling,
                 commit_message=None):
    """Create no-data observations on the sampling grid, within PERIOD only.

    Anchor the grid to the last observation before PERIOD, otherwise the
    first in it. An empty series with both bounds uses start as its anchor.
    Reject off-grid observations rather than adding neighbours to them.
    """
    if sampling is None:
        LOGGER.warning("%s: filling skipped: invalid or missing "
                       "samplingFrequency", datastream.name)
        return 0
    first = start if start is not None else (
        pd.to_datetime(period[0].phenomenon_time, utc=True) if period else None
    )
    last = end if end is not None else (
        pd.to_datetime(period[-1].phenomenon_time, utc=True) if period else None
    )
    if first is None or last is None:
        LOGGER.info("%s: nothing to fill (empty period, no bounds)",
                    datastream.name)
        return 0
    anchor = before[-1] if before else (period[0] if period else None)
    anchor = (pd.to_datetime(anchor.phenomenon_time, utc=True)
              if anchor is not None else first)
    existing = pd.to_datetime([o.phenomenon_time for o in period], utc=True)
    if any((time - anchor) % sampling != pd.Timedelta(0) for time in existing):
        raise ValueError("Observations are not aligned with samplingFrequency; "
                         "filling skipped")
    # Round up to the next grid instant, including one exactly at first.
    first += (anchor - first) % sampling
    missing = pd.date_range(first, last, freq=sampling).difference(existing)
    LOGGER.info("%s: %d missing timestamps on the %s grid from %s to %s",
                datastream.name, len(missing), sampling, first, last)
    created = 0
    for offset in range(0, len(missing), MAX_ROWS_PER_BULK):
        observations = [
            Observation(
                phenomenon_time=time.to_pydatetime(),
                result=NO_DATA,
                result_quality="0",
                datastream=datastream.iot_id,
            )
            for time in missing[offset : offset + MAX_ROWS_PER_BULK]
        ]
        # No force: existing measurements must never be replaced.
        created += writer.bulk_observations(
            observations, commit_message=commit_message, force=False
        )
    return created


def patch_quality(client, datastream_id, changed, commit_message=None):
    """Write the resultQuality of each (observation, quality) of a
    datastream: PATCH /BulkObservations, PAGE at a time, or one PATCH each on
    an istSOS4 that has no bulk PATCH yet (405)."""
    for start in range(0, len(changed), PAGE):
        chunk = changed[start : start + PAGE]
        LOGGER.debug("Datastream %s: bulk PATCH %d-%d of %d", datastream_id,
                     start + 1, start + len(chunk), len(changed))
        try:
            client.bulk_observations_patch(
                [
                    Observation(iot_id=o.iot_id, result_quality=str(q),
                                datastream=datastream_id)
                    for o, q in chunk
                ],
                commit_message=commit_message,
            )
        except requests.HTTPError as error:
            if getattr(error.response, "status_code", None) != 405:
                raise
            LOGGER.warning("Datastream %s: no PATCH /BulkObservations (405), "
                           "falling back to one PATCH per observation for %d",
                           datastream_id, len(changed) - start)
            break
    else:
        return
    # Older istSOS4: one PATCH each, filtered on phenomenonTime (the
    # TimescaleDB partition key), in UTC, single-quoted.
    # ponytail: istSOS returns phenomenonTime to the second but matches it
    # exactly, so one stored with sub-seconds gets 404 and its datastream
    # fails; retry without $filter on 404 if such data shows up
    for o, q in changed[start:]:
        observation = Observation(iot_id=o.iot_id, result_quality=str(q))
        utc = pd.to_datetime(o.phenomenon_time, utc=True).isoformat()
        utc = utc.replace("+00:00", "Z")
        client.patch(observation, filter=f"phenomenonTime eq '{utc}'",
                     commit_message=commit_message)


def check_datastream(client, datastream, start, end, writer=None,
                     fill_commit_message=None, commit_message=None,
                     set_flag=None):
    """PATCH the resultQuality of the datastream's observations in PERIOD."""
    properties = datastream.properties
    constraints = properties["constraints"]
    try:  # kafkaQC: a fixed-length ISO 8601 duration (isodate, not pandas)
        sampling = pd.Timedelta(
            isodate.parse_duration(properties["samplingFrequency"])
        )
        sampling = sampling if sampling > pd.Timedelta(0) else None
    except Exception:
        sampling = None
    # kafkaQC compute_N: largest window / samplingFrequency + 3, else 50
    windows = [
        pd.Timedelta(int(match[1]), match[2].lower())
        for c in constraints
        if isinstance(c.get("window"), str)
        and (match := WINDOW.match(c["window"]))
    ]
    n = 50
    if windows and sampling:
        n = max(1, math.ceil(max(windows) / sampling) + 3)
    LOGGER.info(
        "%s (id %s): %d constraints [%s], samplingFrequency %s, buffer N=%d",
        datastream.name, datastream.iot_id, len(constraints),
        ", ".join(str(c.get("type")) for c in constraints[:4]),
        sampling or "invalid/missing", n,
    )
    if len(constraints) > 4:
        LOGGER.warning("%s: %d constraints, only the first 4 are checked",
                       datastream.name, len(constraints))

    # observations in PERIOD, after the n-1 before it that fill the buffers
    query = f"Datastream/id eq {datastream.iot_id}"
    fields = "id,phenomenonTime,result,resultQuality"
    before = []
    if start is not None:
        older = client.iter_list(
            Observation,
            filter=f"{query} and phenomenonTime lt {start.isoformat()}",
            orderby="phenomenonTime desc",
            select=fields,
            top=n - 1,
        )
        before = list(islice(older, n - 1))[::-1]
        query += f" and phenomenonTime ge {start.isoformat()}"
    if end is not None:
        query += f" and phenomenonTime le {end.isoformat()}"
    period = list(
        client.iter_list(
            Observation,
            filter=query,
            orderby="phenomenonTime",
            select=fields,
            top=PAGE,
        )
    )
    LOGGER.info("%s: %d observations in PERIOD, %d before it to fill buffers",
                datastream.name, len(period), len(before))
    created = 0
    if writer is not None:
        created = fill_missing(
            writer, datastream, before, period, start, end, sampling,
            fill_commit_message,
        )
        if created:
            LOGGER.log(SUCCESS, "%s: %d no-data observations created",
                       datastream.name, created)
            # Read their IDs and include them in the following QC buffers.
            period = list(client.iter_list(
                Observation, filter=query, orderby="phenomenonTime",
                select=fields, top=PAGE,
            ))
    observations = before + period

    times = pd.to_datetime([o.phenomenon_time for o in observations], utc=True)
    times = times.tz_localize(None).to_numpy()
    values = np.array(
        [np.nan if o.result is None else o.result for o in observations], float
    )
    values[np.isin(values, NO_DATA_VALUES)] = np.nan
    targets = np.arange(len(before), len(observations))
    new = []
    if set_flag is not None:  # SET_FLAG: this value everywhere, no QC
        new = [set_flag] * len(period)
    elif period:
        new = quality(
            datastream.name, constraints, times, values, targets, n, sampling
        )

    # PATCH only what changes. Admin/editor users require a commit message;
    # qc users require none.
    changed = [
        (o, q) for o, q in zip(period, new) if str(o.result_quality) != str(q)
    ]
    patch_quality(client, datastream.iot_id, changed, commit_message)
    LOGGER.log(
        SUCCESS, "%s: %d observations checked, %d resultQuality updated",
        datastream.name, len(period), len(changed),
    )
    return len(period), len(changed), created


def main():
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    missing = [key for key in ("ISTSOS4_URL", "ISTSOS4_USERNAME",
                               "ISTSOS4_PASSWORD") if not os.getenv(key)]
    if missing:
        LOGGER.critical("Missing environment variables: %s (wrong --env-file?)",
                        ", ".join(missing))
        sys.exit(1)
    client = Client(
        os.environ["ISTSOS4_URL"],
        os.environ["ISTSOS4_USERNAME"],
        os.environ["ISTSOS4_PASSWORD"],
    )
    # PERIOD="start,end", either may be empty; no offset means UTC (kafkaQC)
    start, _, end = os.getenv("PERIOD", "").partition(",")
    start, end = [
        pd.to_datetime(p.strip(), utc=True) if p.strip() else None
        for p in (start, end)
    ]
    if start is not None and end is not None and start > end:
        raise ValueError("PERIOD start must not be after end")
    commit_message = os.getenv("COMMIT_MESSAGE") or None
    set_flag = os.getenv("SET_FLAG", "").strip()
    if set_flag and not set_flag.isdigit():
        raise ValueError("SET_FLAG must be a non-negative integer")
    set_flag = int(set_flag) if set_flag else None
    writer = None
    fill_enabled = os.getenv("FILL_MISSING", "false").strip().lower()
    if fill_enabled not in ("true", "false"):
        raise ValueError("FILL_MISSING must be true or false")
    if fill_enabled == "true":
        fill_username = os.getenv("ISTSOS4_FILL_USERNAME")
        fill_password = os.getenv("ISTSOS4_FILL_PASSWORD")
        if bool(fill_username) != bool(fill_password):
            raise ValueError("Set both ISTSOS4_FILL_USERNAME and "
                             "ISTSOS4_FILL_PASSWORD, or leave both empty")
        writer = (Client(os.environ["ISTSOS4_URL"], fill_username, fill_password)
                  if fill_username else client)
    fill_commit_message = os.getenv("FILL_COMMIT_MESSAGE") or (
        commit_message if writer is client else None
    )
    names, excluded = [
        [p.strip() for p in os.getenv(key, "").split(",") if p.strip()]
        for key in ("DATASTREAMS", "EXCLUDE_DATASTREAMS")
    ]

    LOGGER.log(
        NOTICE,
        "Starting: %s, PERIOD %s to %s, datastreams %s, excluded %s, "
        "mode %s, fill missing %s, commit message %r",
        os.environ["ISTSOS4_URL"], start or "first", end or "last",
        names or "all with constraints", excluded or "none",
        f"SET_FLAG={set_flag}" if set_flag is not None else "QC",
        fill_enabled, commit_message,
    )

    began = time.monotonic()
    done = failed = skipped = checked = updated = created = 0
    processed = set()
    for datastream in client.list(Datastream):
        if (
            not (datastream.properties or {}).get("constraints")
            or datastream.name in excluded
            or (names and datastream.name not in names)
        ):
            skipped += 1
            LOGGER.debug("%s: skipped (no constraints or filtered out)",
                         datastream.name)
            continue
        processed.add(datastream.name)
        try:  # like Flink, a datastream that fails is reported and skipped
            counts = check_datastream(
                client, datastream, start, end, writer,
                fill_commit_message, commit_message, set_flag,
            )
        except Exception as error:
            failed += 1
            LOGGER.error("%s: %r", datastream.name, error,
                         exc_info=LOGGER.isEnabledFor(logging.DEBUG))
            continue
        done += 1
        checked, updated, created = (
            a + b for a, b in zip((checked, updated, created), counts)
        )
    unseen = [n for n in names if n not in excluded and n not in processed]
    if unseen:
        LOGGER.warning("DATASTREAMS not found or without constraints: %s",
                       ", ".join(unseen))
    LOGGER.log(
        NOTICE,
        "Finished in %.1fs: %d datastreams done, %d failed, %d skipped; "
        "%d observations checked, %d resultQuality updated, %d created",
        time.monotonic() - began, done, failed, skipped,
        checked, updated, created,
    )


if __name__ == "__main__":
    main()
