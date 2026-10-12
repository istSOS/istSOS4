"""Historical QC: recompute resultQuality exactly as the Flink QC job does
(flink/service/job/istsos4/kafkaQC.py and utils/IstsosQC.py), reading the
observations from istSOS4 instead of Kafka.

Flink checks each new observation as the newest of a buffer holding the
previous N-1 observations of its datastream: one SaQC call per constraint and
observation (~10 ms). Here the buffers are laid end to end on a made-up time
axis, far apart, so one SaQC call checks thousands of them.
"""

import math
import os
import re
from itertools import islice

import isodate
import numpy as np
import pandas as pd
from istsos4_client import Client, Datastream, Observation
from saqc import SaQC

NO_DATA_VALUES = [-999, -9999, -999.0, -999.9, -999.99]  # IstsosQC.py
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


def newest_bad(c, times, values, targets, n):
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
        except Exception:  # some buffer breaks it
            bad[part], ran[part] = each(range(start, start + len(newest)))
            continue
        bad[part] = flags[newest]

        for k in sample & set(range(start, start + len(newest))):
            single, own = alone(k), times[first[k] : targets[k] + 1]
            end = newest[k - start] + 1
            stacked = flags[end - len(own) : end]
            far = own >= own[0] + span  # past SaQC start-up effects
            if single is None or (single[far] != stacked[far]).any():
                print(f"  {c['type']} runs one buffer at a time")
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
            print(f"WARNING {name}: slot {slot} {c} not run: {reason}")
            continue
        bad, ran = newest_bad(c, times, values, targets, n)
        valid &= ran
        shift = 2 * slot
        new = (q & ~(3 << shift)) | (np.where(bad, 2, 3) << shift)
        q = np.where(valid & (q != 0), new, q)  # no data (0) stays 0
    return q


def check_datastream(client, datastream, start, end):
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
    observations = before + period

    times = pd.to_datetime([o.phenomenon_time for o in observations], utc=True)
    times = times.tz_localize(None).to_numpy()
    values = np.array(
        [np.nan if o.result is None else o.result for o in observations], float
    )
    values[np.isin(values, NO_DATA_VALUES)] = np.nan
    targets = np.arange(len(before), len(observations))
    new = []
    if period:
        new = quality(
            datastream.name, constraints, times, values, targets, n, sampling
        )

    # PATCH only what changes, with Flink's payload and commit message
    changed = [
        (o, q) for o, q in zip(period, new) if str(o.result_quality) != str(q)
    ]
    for o, q in changed:
        observation = Observation(iot_id=o.iot_id, result_quality=str(q))
        client.patch(observation, commit_message="QC update")
    print(
        f"{datastream.name}: {len(period)} observations checked, "
        f"{len(changed)} resultQuality updated."
    )


def main():
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
    names, excluded = [
        [p.strip() for p in os.getenv(key, "").split(",") if p.strip()]
        for key in ("DATASTREAMS", "EXCLUDE_DATASTREAMS")
    ]

    for datastream in client.list(Datastream):
        if (
            not (datastream.properties or {}).get("constraints")
            or datastream.name in excluded
            or (names and datastream.name not in names)
        ):
            continue
        try:  # like Flink, a datastream that fails is reported and skipped
            check_datastream(client, datastream, start, end)
        except Exception as error:
            print(f"ERROR {datastream.name}: {error!r}")


if __name__ == "__main__":
    main()
