"""Calendar buckets, QC selection and bounded pandas accumulators."""

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
        local = local.replace(minute=local.minute // 10 * 10, second=0, microsecond=0)
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
        floor = bucket_floor(value - timedelta(microseconds=1), interval, offset)
    return floor


def sampling_seconds(properties, mode):
    if mode == "event":
        return None
    try:
        duration = isodate.parse_duration(properties.get("samplingFrequency", ""))
        if not isinstance(duration, timedelta) or duration.total_seconds() <= 0:
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
    def __init__(self, configuration, properties, start, end, cutoff, domain_start):
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
        values, times = [], []
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
            values.append(value)
            times.append(when)
            for i, flags in enumerate(self.checks, start=1):
                flags.add((mask >> (2 * i)) & 3 if mask is not None else 0)
        if not values:
            return
        try:
            frame = pd.DataFrame(
                {"value": values, "time": pd.to_datetime(times, utc=True)}
            )
        except (ValueError, OverflowError):
            raise CalculationError(
                "Observation time is outside the supported pandas range"
            ) from None
        self.sum += float(frame.value.sum())
        self.numeric += len(frame)
        minimum, maximum = float(frame.value.min()), float(frame.value.max())
        self.minimum = minimum if self.minimum is None else min(self.minimum, minimum)
        self.maximum = maximum if self.maximum is None else max(self.maximum, maximum)
        first, last = (
            frame.time.iloc[0].to_pydatetime(),
            frame.time.iloc[-1].to_pydatetime(),
        )
        if self.sampling:
            gaps = frame.time.diff().dt.total_seconds().dropna()
            largest = float(gaps.max()) if len(gaps) else 0
            if self.last is not None:
                largest = max(largest, (first - self.last).total_seconds())
            self.max_gap = max(
                self.max_gap, max(0, math.ceil(largest / self.sampling) - 1)
            )
        self.first = self.first or first
        self.last = last

    def result(self):
        expected = coverage = None
        closure = self.config.get("boundaryMode") or "right_closed"
        if self.sampling:
            # Sample slots are anchored to the full bucket start. Domain start
            # is inclusive; right/left closure decides the bucket edge slots.
            first_slot = max(
                1 if closure == "right_closed" else 0,
                math.ceil(
                    (self.domain_start - self.start).total_seconds() / self.sampling
                ),
            )
            distance = (self.cutoff - self.start).total_seconds() / self.sampling
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
                    math.ceil((self.first - self.start).total_seconds() / self.sampling)
                    - first_slot,
                )
                tail = max(
                    0,
                    last_slot
                    - math.floor(
                        (self.last - self.start).total_seconds() / self.sampling
                    ),
                )
                self.max_gap = max(self.max_gap, head, tail)
        empty_event = self.config["mode"] == "event" and self.total == 0
        valid = self.numeric > 0 or empty_event
        for name, actual in (
            ("minCoverage", coverage),
            ("minObservations", self.numeric),
        ):
            threshold = self.config.get(name)
            if threshold is not None and actual is not None and actual < threshold:
                valid = False
        if (
            self.config.get("maxConsecutiveMissing") is not None
            and self.max_gap > self.config["maxConsecutiveMissing"]
        ):
            valid = False
        if self.numeric and not math.isfinite(self.sum):
            raise CalculationError("Aggregation sum overflowed")
        data_pair = (1 if empty_event else 3) if valid else 0
        mask = data_pair | ((3 if valid else 2) << (2 * (self.n + 2)))
        for i, flags in enumerate(self.checks, start=1):
            pair = 2 if 2 in flags else (3 if flags == {3} else 0)
            mask |= pair << (2 * i)
        # A human has not reviewed this newly computed result: human pair 00.
        return {
            "datastream_id": self.config["datastreamId"],
            "bucket_start": self.start,
            "bucket_end": self.end,
            "evaluated_until": self.cutoff,
            "count_total": self.total,
            "count_accepted": self.accepted,
            "count_numeric": self.numeric,
            "sum_value": self.sum if self.numeric else None,
            "min_value": self.minimum,
            "max_value": self.maximum,
            "avg_value": self.sum / self.numeric if self.numeric else None,
            "expected_count": expected,
            "coverage": coverage,
            "result_quality": mask,
            "constraint_count": self.n,
        }
