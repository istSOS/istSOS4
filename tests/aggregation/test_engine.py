import copy
from threading import Event

import pytest

from aggregation.calculation import TABLES, timestamp
from aggregation.engine import calculate
from aggregation.service import ApiError, Settings


class Source:
    def __init__(self):
        self.rows = [
            {"phenomenonTime": "2026-01-01T00:05:00Z", "result": 2},
            {"phenomenonTime": "2026-01-01T00:10:00Z", "result": 4},
            {"phenomenonTime": "2026-01-01T00:15:00Z", "result": 6},
        ]
        self.fail = False
        self.requests = []
        self.reference_requests = []
        self.reference_end = None
        self.reference_rows = []
        self.reference_changes = []
        self.changes = []
        self.change_requests = []

    def first_observation(self, *args):
        return self.rows[0]["phenomenonTime"] if self.rows else None

    def changed_times(self, identifier, since, until):
        self.change_requests.append((identifier, since, until))
        if identifier == 7:
            return list(self.reference_changes)
        return list(self.changes)

    def last_observation(self, identifier, cutoff, as_of):
        self.reference_requests.append(identifier)
        if identifier == 7:
            return self.reference_end
        return self.rows[-1]["phenomenonTime"] if self.rows else None

    def observations(self, identifier, start, end, lower, upper, as_of):
        rows = self.rows
        if identifier == 7:
            rows = self.reference_rows
        else:
            self.requests.append((start, end, as_of))
        selected = [
            row
            for row in rows
            if (
                timestamp(row["phenomenonTime"]) >= start
                if lower == "ge"
                else timestamp(row["phenomenonTime"]) > start
            )
            and (
                timestamp(row["phenomenonTime"]) <= end
                if upper == "le"
                else timestamp(row["phenomenonTime"]) < end
            )
        ]
        yield selected[:1]
        if self.fail:
            raise ApiError("Page retrieval failed")
        yield selected[1:]


class Store:
    def __init__(self):
        self.cp = None
        self.rows = {}
        self.versioned = False
        self.cutoff = timestamp("2026-01-01T00:20:00Z")
        self.completed = False
        self.batches = []

    def checkpoint(self, _):
        return copy.deepcopy(self.cp)

    def source_context(self):
        return self.cutoff, self.versioned

    def save_buckets(self, identifier, token, table, rows, checkpoint):
        self.batches.append((table, copy.deepcopy(rows)))
        for row in rows:
            self.rows[table, row["bucket_start"]] = copy.deepcopy(row)
        self.cp = copy.deepcopy(checkpoint)

    def daily_rows(self, datastream_id, start, end):
        return [
            row
            for (table, bucket), row in sorted(self.rows.items())
            if table == "observations_1day" and start <= bucket < end
        ]

    def complete(self, identifier, token, checkpoint):
        self.cp = copy.deepcopy(checkpoint)
        self.completed = True


def run(source, store, batch_size=1000, log_results=False, **extra):
    config = {
        "id": 1,
        "datastreamId": 1,
        "mode": "sampled",
        "minInputQuality": 0,
        "utcOffsetMinutes": 0,
        "startTime": "2026-01-01T00:01:00Z",
        **extra,
    }
    return calculate(
        source,
        store,
        config,
        {"properties": {"samplingFrequency": "PT5M"}},
        Settings(
            "http://api/v1",
            write_batch_size=batch_size,
            log_results=log_results,
        ),
        "token",
        Event(),
    )


def test_all_intervals_use_source_values_and_idempotent_upsert():
    source, store = Source(), Store()
    run(source, store)
    assert {key[0] for key in store.rows} == set(TABLES.values())
    hourly = next(
        row
        for (table, _), row in store.rows.items()
        if table == "observations_1hour"
    )
    assert hourly["sum_value"] == 12 and hourly["avg_value"] == 4
    before = copy.deepcopy(store.rows)
    run(source, store)
    assert store.rows == before and store.completed
    assert all((end - start).days <= 30 for start, end, _ in source.requests)


def test_failed_pagination_does_not_write_incomplete_bucket_or_checkpoint():
    source, store = Source(), Store()
    source.fail = True
    with pytest.raises(ApiError):
        run(source, store)
    assert store.cp is None and store.rows == {} and not store.completed
    source.fail = False
    run(source, store)
    assert store.completed


def test_historical_deletion_and_moved_time_recompute_old_and_new_buckets():
    source, store = Source(), Store()
    store.versioned = True
    run(source, store)
    source.rows.pop(0)
    source.rows[0]["phenomenonTime"] = "2026-01-01T00:17:00Z"
    # Old times of the deleted and moved rows, plus the moved row's new time.
    source.changes = [
        timestamp("2026-01-01T00:05:00Z"),
        timestamp("2026-01-01T00:10:00Z"),
        timestamp("2026-01-01T00:17:00Z"),
    ]
    store.cutoff = timestamp("2026-02-01T00:20:00Z")
    # Avoid a month-long bootstrap in this focused test.
    for interval in TABLES:
        store.cp["frontiers"][interval] = "2026-02-02T00:00:00+00:00"
    run(source, store)
    old = store.rows["observations_10min", timestamp("2026-01-01T00:00:00Z")]
    new = store.rows["observations_10min", timestamp("2026-01-01T00:10:00Z")]
    assert old["count_numeric"] == 0 and old["sum_value"] is None
    assert new["sum_value"] == 10
    assert store.cp["changesThrough"] == store.cutoff.isoformat()
    assert all(as_of is not None for _, _, as_of in source.requests)


def test_manual_recompute_does_not_advance_bootstrap_or_change_scan_checkpoint():
    source, store = Source(), Store()
    run(source, store)
    before = copy.deepcopy(store.cp)
    config = {
        "id": 1,
        "datastreamId": 1,
        "mode": "sampled",
        "minInputQuality": 0,
        "utcOffsetMinutes": 0,
    }
    calculate(
        source,
        store,
        config,
        {"properties": {"samplingFrequency": "PT5M"}},
        Settings("http://api/v1"),
        "token",
        Event(),
        manual=(
            timestamp("2026-01-01T00:05:00Z"),
            timestamp("2026-01-01T00:15:00Z"),
        ),
    )
    assert store.cp["frontiers"] == before["frontiers"]
    assert store.cp["changesThrough"] == before["changesThrough"]


def test_backdated_insert_extends_implicit_domain_and_rewinds_bootstrap():
    source, store = Source(), Store()
    run(source, store, startTime=None)
    store.versioned = True
    store.cutoff = timestamp("2026-01-01T00:30:00Z")
    source.rows.insert(
        0, {"phenomenonTime": "2025-12-31T23:55:00Z", "result": 10}
    )
    source.changes = [timestamp("2025-12-31T23:55:00Z")]
    run(source, store, startTime=None)
    assert store.cp["domainStart"] == "2025-12-31T23:55:00+00:00"
    earlier = store.rows[
        "observations_10min", timestamp("2025-12-31T23:50:00Z")
    ]
    assert earlier["sum_value"] == 10


def test_engine_flushes_full_batches_and_final_remainder():
    source, store = Source(), Store()
    store.cutoff = timestamp("2026-01-01T01:00:00Z")
    run(source, store, batch_size=4)
    ten_minute = [
        rows for table, rows in store.batches if table == "observations_10min"
    ]
    assert [len(rows) for rows in ten_minute] == [4, 2]
    assert (
        len(store.rows) == 11
    )  # six 10-minute buckets plus five larger intervals
    assert all(len(rows) <= 4 for _, rows in store.batches)


def test_failed_batch_resumes_at_last_committed_cursor_and_does_not_log_unsaved_rows(
    capsys,
):
    from aggregation.calculation import CalculationError

    class FailingStore(Store):
        fail = True

        def save_buckets(self, *args):
            if self.fail and len(self.batches) == 1:
                raise CalculationError("Bulk transaction rejected")
            super().save_buckets(*args)

    source, store = Source(), FailingStore()
    store.cutoff = timestamp("2026-01-01T01:00:00Z")
    with pytest.raises(CalculationError):
        run(source, store, batch_size=2, log_results=True)
    assert len(store.rows) == 2 and not store.completed
    assert len(capsys.readouterr().out.splitlines()) == 2
    # Only the committed interval advances: the others restart from scratch.
    assert store.cp["frontiers"] == {"PT10M": "2026-01-01T00:20:00+00:00"}
    store.fail = False
    run(source, store, batch_size=2)
    assert store.completed and len(store.rows) == 11


@pytest.mark.parametrize("closure", ["right_closed", "left_closed"])
def test_grouped_reads_match_per_bucket_results_and_reduce_requests(closure):
    source = Source()
    source.rows.extend(
        [
            {"phenomenonTime": "2026-01-01T00:20:00Z", "result": 8},
            {"phenomenonTime": "2026-01-01T00:20:00.500000Z", "result": 16},
        ]
    )
    reference = Store()
    reference.cutoff = timestamp("2026-01-01T06:00:00Z")
    run(source, reference, batch_size=1, boundaryMode=closure)
    assert len(source.requests) == 1  # one read feeds all six intervals
    source.requests.clear()
    grouped = Store()
    grouped.cutoff = reference.cutoff
    run(source, grouped, batch_size=1000, boundaryMode=closure)
    assert grouped.rows == reference.rows
    assert len(source.requests) == 1
    assert grouped.cp == reference.cp


def test_seven_days_of_empty_ten_minute_buckets_need_one_read():
    source, store = Source(), Store()
    source.rows = []
    store.cutoff = timestamp("2026-01-08T00:00:00Z")
    run(source, store, startTime="2026-01-01T00:00:00.000001Z")
    ten_minute = [
        rows for table, rows in store.batches if table == "observations_10min"
    ]
    assert sum(map(len, ten_minute)) == 1008
    assert len(source.requests) == 1  # one window feeds every interval
    assert all(
        row["count_numeric"] == 0 for rows in ten_minute for row in rows
    )


def test_empty_event_bucket_is_zero_only_with_recent_reference_observation():
    source, store = Source(), Store()
    source.rows = [{"phenomenonTime": "2026-01-01T00:05:00Z", "result": 0.2}]
    source.reference_rows = [
        {"phenomenonTime": f"2026-01-01T00:{minute}:00Z"}
        for minute in ("10", "20", "40")
    ]
    source.reference_end = "2026-01-01T00:40:00Z"
    store.cutoff = timestamp("2026-01-01T03:00:00Z")
    run(
        source,
        store,
        mode="event",
        referenceDatastreamId=7,
        referenceMaxAge="PT5M",
    )
    sums = {
        start.strftime("%H:%M"): row["sum_value"]
        for (table, start), row in sorted(store.rows.items())
        if table == "observations_10min"
    }
    # Activity ends at 00:40 + 5 minutes; 00:20-00:30 has no reference
    # observation in its last 5 minutes, so it is missing, not dry.
    assert sums == {
        "00:00": 0.2,
        "00:10": 0.0,
        "00:20": None,
        "00:30": 0.0,
        "00:40": 0.0,
    }
    assert sorted(set(source.reference_requests)) == [1, 7]


def test_late_reference_observation_recomputes_missing_event_bucket():
    source, store = Source(), Store()
    store.versioned = True
    source.rows = [{"phenomenonTime": "2026-01-01T00:05:00Z", "result": 0.2}]
    source.reference_rows = [
        {"phenomenonTime": f"2026-01-01T00:{minute}:00Z"}
        for minute in ("10", "20", "40")
    ]
    source.reference_end = "2026-01-01T00:40:00Z"
    store.cutoff = timestamp("2026-01-01T03:00:00Z")
    extra = {
        "mode": "event",
        "referenceDatastreamId": 7,
        "referenceMaxAge": "PT5M",
    }
    run(source, store, **extra)
    bucket = ("observations_10min", timestamp("2026-01-01T00:20:00Z"))
    assert store.rows[bucket]["sum_value"] is None
    source.reference_rows.append({"phenomenonTime": "2026-01-01T00:28:00Z"})
    source.reference_rows.sort(key=lambda row: row["phenomenonTime"])
    source.reference_changes = ["2026-01-01T00:28:00Z"]
    store.cutoff = timestamp("2026-01-01T03:10:00Z")
    run(source, store, **extra)
    assert store.rows[bucket]["sum_value"] == 0.0


def test_reference_and_source_without_observations_evaluate_nothing():
    source, store = Source(), Store()
    source.rows = []
    run(source, store, mode="event", referenceDatastreamId=7)
    assert store.rows == {} and store.completed


def test_long_bootstrap_reads_each_source_window_once_for_all_intervals():
    source, store = Source(), Store()
    store.cutoff = timestamp("2026-03-15T00:00:00Z")
    run(source, store)
    spans = [(start, end) for start, end, _ in source.requests]
    # 73 days in 30-day windows, each read once and contiguous.
    assert len(spans) == 3
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:]))
    assert all((end - start).days <= 30 for start, end in spans)
    assert (
        store.rows["observations_1month", timestamp("2026-01-01T00:00:00Z")][
            "sum_value"
        ]
        == 12
    )


def test_open_bucket_is_recalculated_when_more_data_arrives():
    source, store = Source(), Store()
    store.cutoff = timestamp("2026-01-01T00:12:00Z")
    run(source, store)
    open_bucket = timestamp("2026-01-01T00:10:00Z")
    first = store.rows["observations_10min", open_bucket]
    assert (
        first["evaluated_until"] == store.cutoff and first["sum_value"] is None
    )
    source.rows.append(
        {"phenomenonTime": "2026-01-01T00:18:00Z", "result": 10}
    )
    store.cutoff = timestamp("2026-01-01T00:30:00Z")
    run(source, store)
    bucket = store.rows["observations_10min", open_bucket]
    assert bucket["sum_value"] == 16
    assert bucket["evaluated_until"] == bucket["bucket_end"]


def test_late_data_without_versioning_follows_the_recompute_window():
    source, store = Source(), Store()
    store.cutoff = timestamp("2026-01-02T00:00:00Z")
    run(source, store, recomputeWindow="PT1H")
    source.rows += [
        {"phenomenonTime": "2026-01-01T00:25:00Z", "result": 100},  # outside
        {"phenomenonTime": "2026-01-01T23:55:00Z", "result": 7},  # inside
    ]
    source.rows.sort(key=lambda row: row["phenomenonTime"])
    store.cutoff = timestamp("2026-01-02T00:10:00Z")
    run(source, store, recomputeWindow="PT1H")
    late = store.rows["observations_10min", timestamp("2026-01-01T23:50:00Z")]
    old = store.rows["observations_10min", timestamp("2026-01-01T00:20:00Z")]
    assert late["sum_value"] == 7
    # Its 10-minute bucket is outside the window: left for manual recomputation.
    assert old["sum_value"] is None
    # Buckets overlapping the window are recalculated in full: the whole day
    # (with both late values), then its week, month and year by rollup.
    day = store.rows["observations_1day", timestamp("2026-01-01T00:00:00Z")]
    year = store.rows["observations_1year", timestamp("2026-01-01T00:00:00Z")]
    assert day["sum_value"] == year["sum_value"] == 2 + 4 + 6 + 100 + 7


def test_recompute_window_order():
    from datetime import timedelta

    from aggregation.engine import recompute_window

    assert recompute_window({"recomputeWindow": "PT2H"}, {}) == timedelta(
        hours=2
    )
    assert recompute_window(
        {}, {"acquisitionFrequency": "PT12H"}
    ) == timedelta(hours=12)
    assert recompute_window({"recomputeWindow": "bad"}, {}) == timedelta(
        days=1
    )
    assert recompute_window({}, {}) == timedelta(days=1)


@pytest.mark.parametrize("mode", ["sampled", "event"])
def test_rollup_matches_direct_calculation_from_observations(mode):
    from datetime import timedelta

    from aggregation.calculation import Accumulator, containing, next_bucket

    source, store = Source(), Store()
    start = timestamp("2026-01-01T00:00:00Z")
    rows = []
    for step in range(9 * 144):  # nine days at 10 minutes, across a Monday
        when = start + timedelta(minutes=10 * (step + 1))
        if step % 37 == 0:
            continue  # missing sample
        value = -999.9 if step % 53 == 0 else (step % 7) * 0.5
        quality = 2 | (2 << 2) if step % 11 == 0 else 3 | (3 << 2)
        rows.append(
            {
                "phenomenonTime": when.isoformat(),
                "result": value,
                "resultQuality": quality,
            }
        )
    source.rows = rows
    store.cutoff = timestamp("2026-01-09T13:00:00Z")
    config = {
        "id": 1,
        "datastreamId": 1,
        "mode": mode,
        "minInputQuality": 0,
        "utcOffsetMinutes": 60,
        "startTime": "2026-01-01T00:00:00Z",
        **({"minCoverage": 0.9} if mode == "sampled" else {}),
    }
    properties = {"samplingFrequency": "PT10M", "constraints": [{}]}
    calculate(
        source,
        store,
        config,
        {"properties": properties},
        Settings("http://api/v1"),
        "token",
        Event(),
    )
    domain_start = timestamp(config["startTime"])
    for interval, table in (
        ("P7D", "observations_7days"),
        ("P1M", "observations_1month"),
        ("P1Y", "observations_1year"),
    ):
        saved = [row for (name, _), row in store.rows.items() if name == table]
        assert saved
        for row in saved:
            begin = row["bucket_start"]
            end = next_bucket(begin, interval, 60)
            direct = Accumulator(
                config,
                properties,
                begin,
                end,
                min(end, store.cutoff),
                domain_start,
            )
            direct.add(
                [
                    item
                    for item in rows
                    if containing(
                        timestamp(item["phenomenonTime"]),
                        interval,
                        60,
                        "right_closed",
                    )
                    == begin
                    and timestamp(item["phenomenonTime"]) <= store.cutoff
                ]
            )
            expected = direct.result()
            for key, value in expected.items():
                if isinstance(value, float):
                    value = pytest.approx(value)
                assert row[key] == value, (interval, begin, key)
