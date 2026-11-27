"""Source-to-bucket calculations with durable cursors and daily rollup.

10 minutes, hours and days are calculated from source observations; weeks,
months and years are combined from the saved days.
"""

import bisect
import hashlib
import heapq
import json
import logging
import time
from collections import defaultdict
from datetime import timedelta

import isodate

from aggregation.calculation import (TABLES, Accumulator, CalculationError,
                                     bucket_floor, containing, next_bucket,
                                     rollup, sampling_seconds, timestamp)

logger = logging.getLogger("aggregation")


READ_WINDOW = timedelta(days=30)
# Calculated from observations; the others are rolled up from the days.
SOURCE_INTERVALS = ("PT10M", "PT1H", "P1D")
ROLLUP_INTERVALS = ("P7D", "P1M", "P1Y")
DEFAULT_RECOMPUTE_WINDOW = timedelta(hours=24)
DEFAULT_REFERENCE_MAX_AGE = timedelta(hours=1)


def recompute_window(configuration, properties):
    """Look-back for late data without versioning: configuration, then the
    Datastream acquisitionFrequency, then 24 hours."""
    for value in (
        configuration.get("recomputeWindow"),
        properties.get("acquisitionFrequency"),
    ):
        if not value:
            continue
        try:
            duration = isodate.parse_duration(value)
        except (isodate.ISO8601Error, TypeError, ValueError):
            continue
        if isinstance(duration, timedelta) and duration > timedelta(0):
            return duration
    return DEFAULT_RECOMPUTE_WINDOW


def reference_max_age(configuration):
    """How long a reference observation proves the station active."""
    value = configuration.get("referenceMaxAge")
    if value:
        try:
            duration = isodate.parse_duration(value)
        except (isodate.ISO8601Error, TypeError, ValueError):
            duration = None
        if isinstance(duration, timedelta) and duration > timedelta(0):
            return duration
    return DEFAULT_REFERENCE_MAX_AGE


def proven(times, cutoff, max_age):
    """A reference observation in [cutoff - max_age, cutoff] (times sorted)."""
    index = bisect.bisect_left(times, cutoff - max_age)
    return index < len(times) and times[index] <= cutoff


def result_record(configuration, interval, row):
    """aggregation_result log line with the values the API would show."""
    valid = (
        (row["result_quality"] >> (2 * (row["constraint_count"] + 2))) & 3
    ) == 3
    return {
        "event": "aggregation_result",
        "datastreamId": configuration["datastreamId"],
        "interval": interval,
        "bucketStart": row["bucket_start"].isoformat(),
        "bucketEnd": row["bucket_end"].isoformat(),
        "evaluatedUntil": row["evaluated_until"].isoformat(),
        "countTotal": row["count_total"],
        "countAccepted": row["count_accepted"],
        "expectedCount": row["expected_count"],
        "coverage": row["coverage"],
        "countNumeric": row["count_numeric"],
        "resultQuality": row["result_quality"],
        "result": {
            key: -999.9 if not valid or row[key] is None else row[key]
            for key in ("sum_value", "avg_value", "min_value", "max_value")
        },
    }


def flush_results(
    state, identifier, run_id, table, pending, checkpoint, log_results=False
):
    if not pending:
        return 0
    started = time.perf_counter()
    state.save_buckets(
        identifier, run_id, table, [row for row, _ in pending], checkpoint
    )
    logger.info(
        "Bulk write: table=%s rows=%s duration=%.3fs",
        table,
        len(pending),
        time.perf_counter() - started,
    )
    # Only report committed results. Failed COPY/merge batches produce no
    # success records and do not advance their checkpoint.
    if log_results:
        print(
            "\n".join(json.dumps(record) for _, record in pending), flush=True
        )
    count = len(pending)
    pending.clear()
    return count


def read_window(
    api, configuration, lower_bound, upper_bound, lower, upper, as_of, stop
):
    """Yield the observation pages of one source window, checking stop."""
    for page in api.observations(
        configuration["datastreamId"],
        lower_bound,
        upper_bound,
        lower,
        upper,
        as_of,
    ):
        if stop.is_set():
            raise CalculationError(
                "Service stopped during observation retrieval"
            )
        yield page


def reference_times(api, reference, start, end, as_of, stop):
    """Sorted reference phenomenon times in [start, end]."""
    times = []
    for page in api.observations(reference, start, end, "ge", "le", as_of):
        if stop.is_set():
            raise CalculationError(
                "Service stopped during reference retrieval"
            )
        # The end of the phenomenon time, as in last_observation().
        times.extend(
            timestamp(item["phenomenonTime"].split("/")[-1]) for item in page
        )
    return sorted(times)


def calculate(
    api, state, configuration, datastream, settings, run_id, stop, manual=None
):
    identifier = configuration["id"]
    properties = datastream.get("properties") or {}
    sampling_seconds(properties, configuration["mode"])
    constraints = properties.get("constraints") or []
    layout = hashlib.sha256(
        json.dumps(constraints, sort_keys=True).encode()
    ).hexdigest()
    checkpoint = state.checkpoint(identifier) or {}
    if checkpoint.get("layout") not in (None, layout) and manual is None:
        raise CalculationError(
            "Source QC layout changed: remap source quality and run a manual recomputation"
        )
    system_cutoff, versioned = state.source_context()
    cutoff = system_cutoff
    if cutoff is None:
        raise CalculationError(
            "Cannot advance versioned processing while prepared transactions exist"
        )
    if configuration.get("endTime"):
        cutoff = min(cutoff, timestamp(configuration["endTime"]))
    as_of = system_cutoff if versioned else None
    reference = configuration.get("referenceDatastreamId")
    max_age = reference_max_age(configuration)
    if reference is not None:
        # Stop at the latest activity (source or reference) plus max age: an
        # empty event bucket is zero only with a reference observation in the
        # max age before its end, otherwise missing.
        ends = [
            timestamp(end)
            for end in (
                api.last_observation(source, cutoff, as_of)
                for source in (reference, configuration["datastreamId"])
            )
            if end is not None
        ]
        if not ends:
            state.complete(identifier, run_id, checkpoint)
            return 0
        cutoff = min(cutoff, max(ends) + max_age)
    closure = configuration.get("boundaryMode") or "right_closed"
    offset = configuration.get("utcOffsetMinutes")
    if offset is None:
        offset = settings.utc_offset_minutes
    domain_start = configuration.get("startTime") or checkpoint.get(
        "domainStart"
    )
    if domain_start is None:
        domain_start = api.first_observation(
            configuration["datastreamId"], cutoff, versioned, system_cutoff
        )
    if domain_start is None:
        state.complete(identifier, run_id, checkpoint)
        return 0
    domain_start = timestamp(domain_start)
    if domain_start > cutoff:
        state.complete(identifier, run_id, checkpoint)
        return 0
    checkpoint.setdefault("frontiers", {})
    checkpoint["domainStart"] = domain_start.isoformat()
    checkpoint["layout"] = layout
    changes = set()
    since = checkpoint.get("changesThrough")
    if versioned and since and manual is None:
        scan_start = timestamp(since) - timedelta(seconds=1)
        if scan_start < system_cutoff:
            changes.update(
                timestamp(value)
                for value in api.changed_times(
                    configuration["datastreamId"], scan_start, system_cutoff
                )
            )
    # A late reference observation proves the buckets ending within max age.
    proofs = []
    if reference is not None and versioned and since and manual is None:
        scan_start = timestamp(since) - timedelta(seconds=1)
        if scan_start < system_cutoff:
            proofs = [
                timestamp(value)
                for value in api.changed_times(
                    reference, scan_start, system_cutoff
                )
            ]
    if configuration.get("startTime") is None and changes:
        earlier = min(changes)
        if earlier < domain_start:
            domain_start = earlier
            checkpoint["domainStart"] = domain_start.isoformat()
            # Backdated inserts can extend an implicit history domain. Rewind
            # bootstrap cursors rather than silently ignoring those old buckets.
            for interval in SOURCE_INTERVALS:
                checkpoint["frontiers"][interval] = containing(
                    domain_start, interval, offset, closure
                ).isoformat()
    # Late data: with versioning the changed times are known exactly;
    # without it, recompute the buckets inside the look-back window.
    window_start = None
    if not versioned and manual is None:
        window_start = max(
            domain_start, cutoff - recompute_window(configuration, properties)
        )
    plans = []
    for interval in SOURCE_INTERVALS:
        table = TABLES[interval]
        last = containing(cutoff, interval, offset, closure)
        initial = containing(domain_start, interval, offset, closure)
        frontier = timestamp(
            checkpoint["frontiers"].get(interval, initial.isoformat())
        )
        recent = {
            containing(point, interval, offset, closure)
            for point in changes
            if domain_start <= point <= cutoff
        }
        for when in proofs:
            if when > cutoff or when + max_age < domain_start:
                continue
            point = containing(max(when, domain_start), interval, offset, closure)
            final = containing(
                min(when + max_age, cutoff), interval, offset, closure
            )
            while point <= final and point < frontier:
                recent.add(point)
                point = next_bucket(point, interval, offset)
        if window_start is not None:
            point = containing(window_start, interval, offset, closure)
            while point < frontier:
                recent.add(point)
                point = next_bucket(point, interval, offset)
        if manual is not None:
            begin, finish = manual
            begin = max(domain_start, begin)
            finish = min(cutoff, finish or cutoff)
            sequence = containing(begin, interval, offset, closure)
            limit = containing(finish, interval, offset, closure)
            behind = []
        else:
            sequence, limit = frontier, last
            behind = sorted(point for point in recent if point < sequence)

        def work(
            sequence=sequence, limit=limit, interval=interval, behind=behind
        ):
            for cursor in behind:
                yield cursor, False
            cursor = sequence
            while cursor <= limit:
                yield cursor, True
                cursor = next_bucket(cursor, interval, offset)

        plans.append((interval, table, work()))

    committed = {
        interval: value
        for interval, value in checkpoint["frontiers"].items()
        if interval in SOURCE_INTERVALS
    }
    updated_at = configuration.get("updatedAt")
    touched_days = set()
    pending = {table: [] for _, table, _ in plans}
    frontier_of = {}
    opened = {}  # (interval, bucket start) -> (accumulator, advances, table)

    def flush(interval, table):
        if frontier_of.get(interval) is not None:
            committed[interval] = frontier_of[interval]
        return flush_results(
            state,
            identifier,
            run_id,
            table,
            pending[table],
            {**checkpoint, "frontiers": dict(committed)},
            settings.log_results,
        )

    def finish(interval, table, start, accumulator, advances, times):
        row = accumulator.result(
            reference is None
            or proven(times, accumulator.cutoff, max_age)
        )
        row["configuration_updated_at"] = updated_at
        if interval == "P1D":
            touched_days.add(start)
        if manual is None and advances:
            # An open bucket (cutoff inside it) is recalculated on the next run.
            closed = accumulator.cutoff >= accumulator.end
            frontier_of[interval] = (
                accumulator.end if closed else start
            ).isoformat()
        pending[table].append(
            (row, result_record(configuration, interval, row))
        )
        if len(pending[table]) >= settings.write_batch_size:
            return flush(interval, table)
        return 0

    def tagged(index, work):
        for start, advances in work:
            yield start, index, advances

    entries = heapq.merge(
        *(tagged(index, work) for index, (_, _, work) in enumerate(plans))
    )
    upcoming = next(entries, None)
    written = 0
    position = lower = None
    while upcoming is not None or opened:
        if stop.is_set():
            raise CalculationError(
                "Service stopped; committed batches will resume next run"
            )
        if not opened:
            position = max(upcoming[0], domain_start)
            lower = (
                "ge"
                if closure == "left_closed"
                or (position == domain_start and domain_start > upcoming[0])
                else "gt"
            )
        reach = position + READ_WINDOW
        horizon = max(
            (accumulator.cutoff for accumulator, _, _ in opened.values()),
            default=position,
        )
        # A bucket starting exactly at the horizon continues the same span.
        while upcoming is not None and (
            not opened or (upcoming[0] <= horizon and upcoming[0] < reach)
        ):
            start, index, advances = upcoming
            interval, table, _ = plans[index]
            end = next_bucket(start, interval, offset)
            accumulator = Accumulator(
                configuration,
                properties,
                start,
                end,
                min(end, cutoff),
                domain_start,
            )
            opened[interval, start] = (accumulator, advances, table)
            horizon = max(horizon, accumulator.cutoff)
            upcoming = next(entries, None)
        upper_bound = min(horizon, reach)
        upper = "lt" if closure == "left_closed" else "le"
        if upper_bound > position or closure == "right_closed":
            started = time.perf_counter()
            pages_read = observations_read = 0
            try:
                for page in read_window(
                    api,
                    configuration,
                    position,
                    upper_bound,
                    lower,
                    upper,
                    as_of,
                    stop,
                ):
                    pages_read += 1
                    observations_read += len(page)
                    partitioned = defaultdict(list)
                    for observation in page:
                        when = timestamp(observation.get("phenomenonTime"))
                        if when < position or when > upper_bound:
                            raise CalculationError(
                                "Source API returned an observation outside the requested window"
                            )
                        for interval in SOURCE_INTERVALS:
                            key = (
                                interval,
                                containing(when, interval, offset, closure),
                            )
                            if key in opened:
                                partitioned[key].append(observation)
                    for key, values in partitioned.items():
                        opened[key][0].add(values)
            except CalculationError as error:
                error.context.update(
                    step="observation_window",
                    windowStart=position.isoformat(),
                    windowEnd=upper_bound.isoformat(),
                )
                raise
            logger.info(
                "Window read: buckets=%s pages=%s observations=%s duration=%.3fs",
                len(opened),
                pages_read,
                observations_read,
                time.perf_counter() - started,
            )
        # Buckets evaluated up to this window are complete, in time order.
        ready = sorted(
            (
                key
                for key, item in opened.items()
                if item[0].cutoff <= upper_bound
            ),
            key=lambda key: (key[1], SOURCE_INTERVALS.index(key[0])),
        )
        times = []
        if reference is not None and any(
            opened[key][0].total == 0 for key in ready
        ):
            times = reference_times(
                api, reference, position - max_age, upper_bound, as_of, stop
            )
        for key in ready:
            accumulator, advances, table = opened.pop(key)
            written += finish(
                key[0], table, key[1], accumulator, advances, times
            )
        position = upper_bound
        lower = "ge" if closure == "left_closed" else "gt"
    for interval, table, _ in plans:
        written += flush(interval, table)
    checkpoint["frontiers"] = committed
    written += roll_up(
        state,
        configuration,
        properties,
        identifier,
        run_id,
        checkpoint,
        settings,
        touched_days,
        offset,
        cutoff,
        lambda end: reference is None
        or proven(
            reference_times(api, reference, end - max_age, end, as_of, stop),
            end,
            max_age,
        ),
    )
    if manual is None:
        checkpoint["changesThrough"] = system_cutoff.isoformat()
    # All bucket results are already durable; final checkpoint/status is atomic.
    state.complete(identifier, run_id, checkpoint)
    return written


def roll_up(
    state,
    configuration,
    properties,
    identifier,
    run_id,
    checkpoint,
    settings,
    days,
    offset,
    cutoff,
    active=lambda end: True,
):
    """Rebuild the weeks, months and years containing the recalculated days.

    active(end) tells whether an empty event bucket ending at end is zero.
    """
    if not days:
        return 0
    constraint_count = len(properties.get("constraints") or [])
    buckets = {
        (interval, bucket_floor(day, interval, offset))
        for day in days
        for interval in ROLLUP_INTERVALS
    }
    lowest = min(start for _, start in buckets)
    highest = max(
        next_bucket(start, interval, offset) for interval, start in buckets
    )
    saved = state.daily_rows(configuration["datastreamId"], lowest, highest)
    written = 0
    for interval in ROLLUP_INTERVALS:
        table = TABLES[interval]
        pending = []
        for _, start in sorted(
            item for item in buckets if item[0] == interval
        ):
            end = next_bucket(start, interval, offset)
            evaluated = min(end, cutoff)
            parts = [d for d in saved if start <= d["bucket_start"] < evaluated]
            empty = not any(d["count_total"] for d in parts)
            row = rollup(
                configuration,
                constraint_count,
                start,
                end,
                evaluated,
                parts,
                configuration["mode"] != "event"
                or not empty
                or active(evaluated),
            )
            row["configuration_updated_at"] = configuration.get("updatedAt")
            pending.append((row, result_record(configuration, interval, row)))
            if len(pending) >= settings.write_batch_size:
                written += flush_results(
                    state,
                    identifier,
                    run_id,
                    table,
                    pending,
                    checkpoint,
                    settings.log_results,
                )
        written += flush_results(
            state,
            identifier,
            run_id,
            table,
            pending,
            checkpoint,
            settings.log_results,
        )
    return written
