"""Calendar buckets, QC selection, bucket accumulators and daily rollup."""

import math
from datetime import UTC, datetime, timedelta, timezone
from numbers import Real

import isodate
import pandas as pd

TABLES = dict(
    zip(
        ("PT10M", "PT1H", "P1D", "P7D", "P1M", "P1Y"),
        (
            "observations_10min",
            "observations_1hour",
            "observations_1day",
            "observations_7days",
            "observations_1month",
            "observations_1year",
        ),
        strict=True,
    )
)


class CalculationError(Exception):
    def __init__(self, message, **context):
        super().__init__(message)
        self.context = context


def timestamp(value):
    if isinstance(value, datetime):
        result = value
    else:
        if not isinstance(value, str):
            raise CalculationError("Observation time must be an ISO datetime")
        if "/" in value:
            left, right = value.split("/", 1)
            if left != right:
                raise CalculationError(
                    "Interval-valued observations require an explicit allocation rule; only point observations are supported"
                )
            value = left
        try:
            result = datetime.fromisoformat(value)
        except ValueError:
            raise CalculationError("Invalid observation datetime") from None
    if result.tzinfo is None:
        raise CalculationError("Observation time requires a timezone")
    return result.astimezone(UTC)


def bucket_floor(value, interval, offset):
    local = value.astimezone(timezone(timedelta(minutes=offset)))
    if interval == "PT10M":
        local = local.replace(
            minute=local.minute // 10 * 10, second=0, microsecond=0
        )
    elif interval == "PT1H":
        local = local.replace(minute=0, second=0, microsecond=0)
    else:
        local = local.replace(hour=0, minute=0, second=0, microsecond=0)
        if interval == "P7D":
            local -= timedelta(days=local.weekday())
        elif interval == "P1M":
            local = local.replace(day=1)
        elif interval == "P1Y":
            local = local.replace(month=1, day=1)
    return local.astimezone(UTC)


def next_bucket(start, interval, offset=0):
    local = start.astimezone(timezone(timedelta(minutes=offset)))
    if interval == "P1M":
        return (
            (pd.Timestamp(local) + pd.DateOffset(months=1))
            .to_pydatetime()
            .astimezone(UTC)
        )
    if interval == "P1Y":
        return (
            (pd.Timestamp(local) + pd.DateOffset(years=1))
            .to_pydatetime()
            .astimezone(UTC)
        )
    return (
        start
        + {
            "PT10M": timedelta(minutes=10),
            "PT1H": timedelta(hours=1),
            "P1D": timedelta(days=1),
            "P7D": timedelta(days=7),
        }[interval]
    )


def containing(value, interval, offset, closure):
    floor = bucket_floor(value, interval, offset)
    if closure == "right_closed" and floor == value:
        floor = bucket_floor(
            value - timedelta(microseconds=1), interval, offset
        )
    return floor


def sampling_seconds(properties, mode):
    if mode == "event":
        return None
    try:
        duration = isodate.parse_duration(
            properties.get("samplingFrequency", "")
        )
        if (
            not isinstance(duration, timedelta)
            or duration.total_seconds() <= 0
        ):
            raise ValueError
        return duration.total_seconds()
    except (ValueError, TypeError, isodate.ISO8601Error):
        raise CalculationError(
            "Sampled Datastream requires a positive fixed properties.samplingFrequency"
        ) from None


def quality(value, configuration, count):
    if configuration.get("ignoreQuality", False):
        return True, None
    key = configuration.get("resultQualityKey")
    if key is not None:
        if value is None:
            value = None
        elif not isinstance(value, dict):
            return False, None
        else:
            value = value.get(key)
    if value is None:
        return configuration["minInputQuality"] == 0, None
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return False, None
    try:
        mask = int(value)
    except ValueError:
        return False, None
    if mask < 0 or mask >> (2 * (count + 2)):
        return False, None
    checks = [(mask >> (2 * i)) & 3 for i in range(1, count + 1)]
    human = (mask >> (2 * (count + 1))) & 3
    if human == 2:
        return False, mask
    if human == 3:
        return True, mask
    if (mask & 3) == 0 or 1 in checks or human == 1:
        return False, mask
    if checks and all(check == 3 for check in checks):
        level = 3
    elif checks and all(check == 2 for check in checks):
        level = 1
    elif any(check in (2, 3) for check in checks):
        level = 2
    else:
        level = 0
    return level >= configuration["minInputQuality"], mask


class Accumulator:
    def __init__(
        self, configuration, properties, start, end, cutoff, domain_start
    ):
        self.config = configuration
        self.start, self.end, self.cutoff = start, end, cutoff
        self.domain_start = domain_start
        constraints = properties.get("constraints")
        if constraints is None:
            constraints = []
        if (
            not isinstance(constraints, list)
            or not all(isinstance(c, dict) for c in constraints)
            or len(constraints) > 28
        ):
            raise CalculationError(
                "QC constraints must be an array of at most 28 objects"
            )
        self.constraints = constraints
        self.n = len(constraints)
        self.sampling = sampling_seconds(properties, configuration["mode"])
        self.total = self.accepted = self.numeric = 0
        self.sum = 0.0
        self.minimum = self.maximum = self.first = self.last = None
        self.max_gap = 0
        self.checks = [set() for _ in constraints]
        self.factor = configuration.get("conversionFactor")
        if self.factor is None:
            self.factor = 1.0
        self.markers = configuration.get("missingValues")
        if self.markers is None:
            self.markers = [-999.9]

    def add(self, observations):
        """Add observations in phenomenonTime order (one page at a time)."""
        for observation in observations:
            when = timestamp(observation.get("phenomenonTime"))
            self.total += 1
            accepted, mask = quality(
                observation.get("resultQuality"), self.config, self.n
            )
            if not accepted:
                continue
            self.accepted += 1
            value = observation.get("result")
            if value is None and self.config["mode"] == "event":
                value = 1.0  # A tick without a value counts as one event.
            if isinstance(value, bool) or not isinstance(value, Real):
                continue
            try:
                value = float(value)
            except OverflowError:
                continue
            if not math.isfinite(value) or value in self.markers:
                continue
            value *= self.factor
            if not math.isfinite(value):
                raise CalculationError("Numeric conversion overflowed")
            for i, flags in enumerate(self.checks, start=1):
                flags.add((mask >> (2 * i)) & 3 if mask is not None else 0)
            self.numeric += 1
            self.sum += value
            if self.minimum is None or value < self.minimum:
                self.minimum = value
            if self.maximum is None or value > self.maximum:
                self.maximum = value
            if self.sampling and self.last is not None:
                missing = (
                    math.ceil(
                        (when - self.last).total_seconds() / self.sampling
                    )
                    - 1
                )
                self.max_gap = max(self.max_gap, missing)
            if self.first is None:
                self.first = when
            self.last = when

    def result(self, proven=True):
        """proven: False when an empty event bucket has no reference proof."""
        expected = coverage = None
        closure = self.config.get("boundaryMode") or "right_closed"
        if self.sampling:
            # Sample slots are anchored to the full bucket start. Domain start
            # is inclusive; right/left closure decides the bucket edge slots.
            first_slot = max(
                1 if closure == "right_closed" else 0,
                math.ceil(
                    (self.domain_start - self.start).total_seconds()
                    / self.sampling
                ),
            )
            distance = (
                self.cutoff - self.start
            ).total_seconds() / self.sampling
            last_slot = (
                math.floor(distance)
                if closure == "right_closed"
                else math.ceil(distance) - 1
            )
            expected = max(0, last_slot - first_slot + 1)
            coverage = self.numeric / expected if expected else None
            if self.first is None:
                self.max_gap = expected
            else:
                head = max(
                    0,
                    math.ceil(
                        (self.first - self.start).total_seconds()
                        / self.sampling
                    )
                    - first_slot,
                )
                tail = max(
                    0,
                    last_slot
                    - math.floor(
                        (self.last - self.start).total_seconds()
                        / self.sampling
                    ),
                )
                self.max_gap = max(self.max_gap, head, tail)
        empty_event = self.config["mode"] == "event" and self.total == 0
        valid = is_valid(
            self.config, self.numeric, coverage, empty_event and proven
        )
        if (
            self.config.get("maxConsecutiveMissing") is not None
            and self.max_gap > self.config["maxConsecutiveMissing"]
        ):
            valid = False
        pairs = [
            2 if 2 in flags else (3 if flags == {3} else 0)
            for flags in self.checks
        ]
        return result_row(
            self.config,
            self.start,
            self.end,
            self.cutoff,
            (self.total, self.accepted, self.numeric),
            (self.sum, self.minimum, self.maximum),
            expected,
            coverage,
            output_mask(valid, empty_event, self.n, pairs),
            self.n,
        )


def is_valid(configuration, numeric, coverage, empty_event):
    """Data present (or an empty event bucket) and completeness thresholds met."""
    valid = numeric > 0 or empty_event
    for name, actual in (
        ("minCoverage", coverage),
        ("minObservations", numeric),
    ):
        threshold = configuration.get(name)
        if threshold is not None and actual is not None and actual < threshold:
            valid = False
    return valid


def output_mask(valid, empty_event, constraint_count, pairs):
    """Pairs: data, one per automatic check, human (00: not reviewed), aggregation."""
    data_pair = (1 if empty_event else 3) if valid else 0
    mask = data_pair | ((3 if valid else 2) << (2 * (constraint_count + 2)))
    for i, pair in enumerate(pairs, start=1):
        mask |= pair << (2 * i)
    return mask


def result_row(
    configuration,
    start,
    end,
    cutoff,
    counts,
    statistics,
    expected,
    coverage,
    mask,
    constraint_count,
):
    total, accepted, numeric = counts
    total_sum, minimum, maximum = statistics
    if numeric and not math.isfinite(total_sum):
        raise CalculationError("Aggregation sum overflowed")
    # Only a valid empty event bucket is worth zero; anything else without
    # values (sampled gap, rejected or unproven events) stays missing.
    zero = 0.0 if (mask & 3) == 1 else None
    return {
        "datastream_id": configuration["datastreamId"],
        "bucket_start": start,
        "bucket_end": end,
        "evaluated_until": cutoff,
        "count_total": total,
        "count_accepted": accepted,
        "count_numeric": numeric,
        "sum_value": total_sum if numeric else zero,
        "min_value": minimum if numeric else zero,
        "max_value": maximum if numeric else zero,
        "avg_value": total_sum / numeric if numeric else zero,
        "expected_count": expected,
        "coverage": coverage,
        "result_quality": mask,
        "constraint_count": constraint_count,
    }


def rollup(
    configuration, constraint_count, start, end, cutoff, days, proven=True
):
    """Combine saved daily rows into one week, month or year bucket.

    Counts, sums, min and max combine exactly; coverage and validity are
    recomputed from the summed counts. maxConsecutiveMissing is not applied:
    gaps across day edges cannot be rebuilt from daily rows.
    """
    total = sum(day["count_total"] for day in days)
    accepted = sum(day["count_accepted"] for day in days)
    numeric = sum(day["count_numeric"] for day in days)
    filled = [day for day in days if day["count_numeric"]]
    statistics = (
        sum(day["sum_value"] for day in filled),
        min((day["min_value"] for day in filled), default=None),
        max((day["max_value"] for day in filled), default=None),
    )
    expected = coverage = None
    if configuration["mode"] == "sampled":
        expected = sum(day["expected_count"] or 0 for day in days)
        coverage = numeric / expected if expected else None
    empty_event = configuration["mode"] == "event" and total == 0
    valid = is_valid(configuration, numeric, coverage, empty_event and proven)
    # Same rule as for observations: failed anywhere, passed everywhere, else 00.
    pairs = []
    for i in range(1, constraint_count + 1):
        flags = {(day["result_quality"] >> (2 * i)) & 3 for day in filled}
        pairs.append(2 if 2 in flags else (3 if flags == {3} else 0))
    return result_row(
        configuration,
        start,
        end,
        cutoff,
        (total, accepted, numeric),
        statistics,
        expected,
        coverage,
        output_mask(valid, empty_event, constraint_count, pairs),
        constraint_count,
    )
