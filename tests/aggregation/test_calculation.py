from datetime import UTC, datetime, timedelta

import pytest

from aggregation.calculation import (
    Accumulator,
    CalculationError,
    bucket_floor,
    containing,
    next_bucket,
    quality,
    timestamp,
)


def config(**extra):
    return {
        "id": 1,
        "datastreamId": 1,
        "mode": "sampled",
        "minInputQuality": 0,
        "ignoreQuality": False,
        **extra,
    }


def accumulator(**extra):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return Accumulator(
        config(**extra),
        {"samplingFrequency": "PT1M"},
        start,
        start + timedelta(minutes=10),
        start + timedelta(minutes=10),
        start,
    )


def observation(minute, value, qc=None):
    return {
        "phenomenonTime": f"2026-01-01T00:{minute:02}:00Z",
        "result": value,
        "resultQuality": qc,
    }


def test_pandas_streaming_statistics_conversion_and_missing_values():
    result = accumulator(conversionFactor=2)
    result.add(
        [
            observation(1, 1),
            observation(2, -999.9),
            observation(3, None),
            observation(4, True),
        ]
    )
    result.add([observation(5, 3), observation(6, "4")])
    row = result.result()
    assert (row["count_total"], row["count_accepted"], row["count_numeric"]) == (
        6,
        6,
        2,
    )
    assert (row["sum_value"], row["avg_value"], row["min_value"], row["max_value"]) == (
        8,
        4,
        2,
        6,
    )
    assert row["expected_count"] == 10 and row["coverage"] == 0.2


@pytest.mark.parametrize(
    "value,count,threshold,accepted",
    [
        (None, 0, 0, True),
        (None, 0, 1, False),
        (3, 0, 0, True),
        (3, 0, 3, False),
        (15, 1, 3, True),
        (11, 1, 1, True),
        (11, 1, 2, False),
        (51, 1, 3, True),
        (55, 1, 3, True),
        (47, 1, 0, False),
        (-1, 0, 0, False),
        (True, 0, 0, False),
        ("bad", 0, 0, False),
        (4, 0, 0, False),
        (16, 0, 0, False),
        ({"code": 3}, 0, 0, False),
    ],
)
def test_quality_threshold_and_human_override(value, count, threshold, accepted):
    assert quality(value, config(minInputQuality=threshold), count)[0] is accepted


def test_quality_key_and_full_bypass():
    assert quality({"code": 15}, config(minInputQuality=3, resultQualityKey="code"), 1)[
        0
    ]
    for value in (None, -1, "bad", True, 47):
        assert quality(value, config(ignoreQuality=True, minInputQuality=3), 1)[0]
    result = accumulator(ignoreQuality=True)
    result.add([observation(1, -999.9), observation(2, 2, "bad")])
    assert result.result()["count_numeric"] == 1


def test_empty_events_are_distinct_from_rejected_events():
    empty = accumulator(mode="event", minInputQuality=3).result()
    assert empty["count_numeric"] == 0 and empty["sum_value"] is None
    assert empty["expected_count"] is None and empty["coverage"] is None
    assert empty["result_quality"] & 3 == 1
    assert empty["result_quality"] >> 4 == 3
    rejected = accumulator(mode="event", minInputQuality=3)
    rejected.add([observation(1, 1)])
    assert rejected.result()["result_quality"] & 3 == 0
    assert rejected.result()["result_quality"] >> 4 == 2


def test_completeness_failure_retains_real_statistics_without_marker_pollution():
    result = accumulator(minCoverage=0.8)
    result.add([observation(1, 10)])
    row = result.result()
    assert row["sum_value"] == 10 and row["count_numeric"] == 1
    assert row["result_quality"] & 3 == 0
    assert row["result_quality"] >> 4 == 2


def test_gaps_across_pages_and_open_bucket_expectation():
    result = accumulator(maxConsecutiveMissing=2)
    result.cutoff -= timedelta(minutes=5)
    result.add([observation(1, 2)])
    result.add([observation(5, 3)])
    row = result.result()
    assert row["expected_count"] == 5
    assert result.max_gap == 3 and row["result_quality"] & 3 == 0


@pytest.mark.parametrize(
    "interval,offset,start,end",
    [
        ("P1M", 60, "2026-02-28T23:00:00Z", "2026-03-31T23:00:00Z"),
        ("P1M", -300, "2024-02-01T05:00:00Z", "2024-03-01T05:00:00Z"),
        ("P1Y", 60, "2023-12-31T23:00:00Z", "2024-12-31T23:00:00Z"),
    ],
)
def test_calendar_periods_respect_fixed_offset(interval, offset, start, end):
    assert next_bucket(timestamp(start), interval, offset) == timestamp(end)


def test_monday_alignment_and_right_closed_boundaries():
    monday = timestamp("2026-11-08T23:00:00Z")
    assert bucket_floor(timestamp("2026-11-12T12:00:00Z"), "P7D", 60) == monday
    edge = timestamp("2026-01-01T00:10:00Z")
    assert containing(edge, "PT10M", 0, "right_closed") == edge - timedelta(minutes=10)
    assert containing(edge, "PT10M", 0, "left_closed") == edge


def test_invalid_sampling_or_interval_observation_fail_explicitly():
    with pytest.raises(CalculationError):
        timestamp("2026-01-01T00:00:00Z/2026-01-01T01:00:00Z")
    with pytest.raises(CalculationError):
        Accumulator(
            config(),
            {"samplingFrequency": "P1M"},
            timestamp("2026-01-01T00:00Z"),
            timestamp("2026-02-01T00:00Z"),
            timestamp("2026-01-02T00:00Z"),
            timestamp("2026-01-01T00:00Z"),
        )
