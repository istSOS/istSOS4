"""Sequential source-to-bucket calculations with durable bootstrap cursors."""

import hashlib
import json
import logging
import time
from collections import defaultdict
from datetime import timedelta

from aggregation.calculation import (
    TABLES,
    Accumulator,
    CalculationError,
    bucket_floor,
    containing,
    next_bucket,
    sampling_seconds,
    timestamp,
)

logger = logging.getLogger("aggregation")


def flush_results(state, identifier, run_id, table, pending, checkpoint):
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
    print("\n".join(json.dumps(record) for _, record in pending), flush=True)
    count = len(pending)
    pending.clear()
    return count


def bucket_groups(entries, interval, offset, limit):
    """Group adjacent buckets for one window read instead of one read per bucket.

    Keep at most seven days or `limit` accumulators, except a single calendar
    bucket can span longer and is itself read in seven-day windows.
    """
    group = []
    for start, advances in entries:
        if group and (
            len(group) >= limit
            or start != next_bucket(group[-1][0], interval, offset)
            or next_bucket(start, interval, offset) - group[0][0] > timedelta(days=7)
        ):
            yield group
            group = []
        group.append((start, advances))
    if group:
        yield group


def read_bucket_group(
    api,
    configuration,
    properties,
    group,
    interval,
    offset,
    closure,
    domain_start,
    cutoff,
    as_of,
    stop,
):
    started = time.perf_counter()
    pages_read = observations_read = 0
    accumulators = {}
    for start, _ in group:
        end = next_bucket(start, interval, offset)
        accumulators[start] = Accumulator(
            configuration, properties, start, end, min(end, cutoff), domain_start
        )
    first = group[0][0]
    evaluated = min(next_bucket(group[-1][0], interval, offset), cutoff)
    window = max(first, domain_start)
    while window < evaluated or (window == evaluated and closure == "right_closed"):
        if stop.is_set():
            raise CalculationError("Service stopped during observation retrieval")
        upper = min(window + timedelta(days=7), evaluated)
        include_start = window == domain_start and window > first
        lower_operator = "ge" if closure == "left_closed" or include_start else "gt"
        upper_operator = "lt" if closure == "left_closed" else "le"
        try:
            for page in api.observations(
                configuration["datastreamId"],
                window,
                upper,
                lower_operator,
                upper_operator,
                as_of,
            ):
                if stop.is_set():
                    raise CalculationError(
                        "Service stopped during observation retrieval"
                    )
                pages_read += 1
                observations_read += len(page)
                partitioned = defaultdict(list)
                for observation in page:
                    key = containing(
                        timestamp(observation.get("phenomenonTime")),
                        interval,
                        offset,
                        closure,
                    )
                    if key not in accumulators:
                        raise CalculationError(
                            "Source API returned an observation outside the requested bucket group"
                        )
                    partitioned[key].append(observation)
                for key, values in partitioned.items():
                    accumulators[key].add(values)
        except CalculationError as error:
            error.context.update(
                step="observation_window",
                interval=interval,
                windowStart=window.isoformat(),
                windowEnd=upper.isoformat(),
            )
            raise
        if upper == evaluated:
            break
        window = upper
    logger.info(
        "Window read: interval=%s buckets=%s pages=%s observations=%s duration=%.3fs",
        interval,
        len(group),
        pages_read,
        observations_read,
        time.perf_counter() - started,
    )
    return accumulators


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
    closure = configuration.get("boundaryMode") or "right_closed"
    offset = configuration.get("utcOffsetMinutes")
    if offset is None:
        offset = settings.utc_offset_minutes
    domain_start = configuration.get("startTime") or checkpoint.get("domainStart")
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
            for page in api.changed_observations(
                configuration["datastreamId"], scan_start, system_cutoff
            ):
                for observation in page:
                    validity = observation.get("systemTimeValidity")
                    if not isinstance(validity, str):
                        raise CalculationError(
                            "Historical API response has no systemTimeValidity"
                        )
                    boundaries = validity.split("/")
                    changed = any(
                        scan_start <= timestamp(edge) <= system_cutoff
                        for edge in boundaries
                        if edge != "infinity"
                    )
                    if changed:
                        changes.add(timestamp(observation["phenomenonTime"]))
    if configuration.get("startTime") is None and changes:
        earlier = min(changes)
        if earlier < domain_start:
            domain_start = earlier
            checkpoint["domainStart"] = domain_start.isoformat()
            # Backdated inserts can extend an implicit history domain. Rewind
            # bootstrap cursors rather than silently ignoring those old buckets.
            for interval in TABLES:
                checkpoint["frontiers"][interval] = containing(
                    domain_start, interval, offset, closure
                ).isoformat()
    snapshot = {
        "configuration": configuration,
        "properties": properties,
        "effectiveUtcOffsetMinutes": offset,
        "boundaryMode": closure,
        "conversionFactor": configuration.get("conversionFactor")
        if configuration.get("conversionFactor") is not None
        else 1,
        "missingValues": configuration.get("missingValues")
        if configuration.get("missingValues") is not None
        else [-999.9],
        "aggregationStatusPairs": {"notComputed": 0, "failed": 2, "passed": 3},
        "sourceVersioning": versioned,
    }
    written = 0
    for interval, table in TABLES.items():
        pending = []
        last = containing(cutoff, interval, offset, closure)
        initial = containing(domain_start, interval, offset, closure)
        frontier = timestamp(checkpoint["frontiers"].get(interval, initial.isoformat()))
        recent = set()
        point = last
        for _ in range(configuration.get("bucketsToRecompute") or 3):
            if point >= initial:
                recent.add(point)
            point = bucket_floor(point - timedelta(microseconds=1), interval, offset)
        recent.update(
            containing(point, interval, offset, closure)
            for point in changes
            if domain_start <= point <= cutoff
        )
        if manual is not None:
            begin, finish = manual
            begin = max(domain_start, begin)
            finish = min(cutoff, finish or cutoff)
            sequence = containing(begin, interval, offset, closure)
            limit = containing(finish, interval, offset, closure)
        else:
            sequence, limit = frontier, last

        def work(sequence=sequence, limit=limit, interval=interval, recent=recent):
            cursor = sequence
            while cursor <= limit:
                yield cursor, True
                cursor = next_bucket(cursor, interval, offset)
            if manual is None:
                for cursor in sorted(recent):
                    if not sequence <= cursor <= limit:
                        yield cursor, False

        for group in bucket_groups(work(), interval, offset, settings.write_batch_size):
            if stop.is_set():
                raise CalculationError(
                    "Service stopped; committed batches will resume next run"
                )
            accumulators = read_bucket_group(
                api,
                configuration,
                properties,
                group,
                interval,
                offset,
                closure,
                domain_start,
                cutoff,
                system_cutoff if versioned else None,
                stop,
            )
            for start, advances in group:
                accumulator = accumulators[start]
                end = accumulator.end
                evaluated = accumulator.cutoff
                row = accumulator.result()
                row["configuration_snapshot"] = snapshot
                if manual is None and advances:
                    checkpoint["frontiers"][interval] = end.isoformat()
                valid = (
                    (row["result_quality"] >> (2 * (row["constraint_count"] + 2))) & 3
                ) == 3
                empty = configuration["mode"] == "event" and row["count_total"] == 0
                display = {
                    key: (0 if empty else -999.9)
                    if not valid or value is None
                    else value
                    for key, value in row.items()
                    if key in ("sum_value", "avg_value", "min_value", "max_value")
                }
                record = {
                    "event": "aggregation_result",
                    "datastreamId": configuration["datastreamId"],
                    "interval": interval,
                    "bucketStart": start.isoformat(),
                    "bucketEnd": end.isoformat(),
                    "evaluatedUntil": evaluated.isoformat(),
                    "countTotal": row["count_total"],
                    "countAccepted": row["count_accepted"],
                    "expectedCount": row["expected_count"],
                    "coverage": row["coverage"],
                    "countNumeric": row["count_numeric"],
                    "resultQuality": row["result_quality"],
                    "result": display,
                }
                pending.append((row, record))
                if len(pending) >= settings.write_batch_size:
                    written += flush_results(
                        state, identifier, run_id, table, pending, checkpoint
                    )
        written += flush_results(state, identifier, run_id, table, pending, checkpoint)
    if manual is None:
        checkpoint["changesThrough"] = system_cutoff.isoformat()
    # All bucket results are already durable; final checkpoint/status is atomic.
    state.complete(identifier, run_id, checkpoint)
    return written
