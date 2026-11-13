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
        self.changes = []
        self.fail = False
        self.requests = []

    def first_observation(self, *args):
        return self.rows[0]["phenomenonTime"] if self.rows else None

    def observations(self, identifier, start, end, lower, upper, as_of):
        self.requests.append((start, end, as_of))
        selected = [
            row
            for row in self.rows
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

    def changed_observations(self, *args):
        yield self.changes


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

    def complete(self, identifier, token, checkpoint):
        self.cp = copy.deepcopy(checkpoint)
        self.completed = True


def run(source, store, batch_size=1000, **extra):
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
        Settings("http://api/v1", write_batch_size=batch_size),
        "token",
        Event(),
    )


def test_all_intervals_use_source_values_and_idempotent_upsert():
    source, store = Source(), Store()
    run(source, store)
    assert {key[0] for key in store.rows} == set(TABLES.values())
    hourly = next(
        row for (table, _), row in store.rows.items() if table == "observations_1hour"
    )
    assert hourly["sum_value"] == 12 and hourly["avg_value"] == 4
    before = copy.deepcopy(store.rows)
    run(source, store)
    assert store.rows == before and store.completed
    assert all((end - start).days <= 7 for start, end, _ in source.requests)


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
    source.changes = [
        {
            "phenomenonTime": "2026-01-01T00:05:00Z",
            "systemTimeValidity": "2025-01-01T00:00:00Z/2026-01-01T00:21:00Z",
        },
        {
            "phenomenonTime": "2026-01-01T00:10:00Z",
            "systemTimeValidity": "2025-01-01T00:00:00Z/2026-01-01T00:21:00Z",
        },
        {
            "phenomenonTime": "2026-01-01T00:17:00Z",
            "systemTimeValidity": "2026-01-01T00:21:00Z/infinity",
        },
    ]
    store.cutoff = timestamp("2026-02-01T00:20:00Z")
    # Avoid a month-long bootstrap in this focused test.
    for interval in TABLES:
        store.cp["frontiers"][interval] = "2026-02-02T00:00:00+00:00"
    run(source, store, bucketsToRecompute=1)
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
        manual=(timestamp("2026-01-01T00:05:00Z"), timestamp("2026-01-01T00:15:00Z")),
    )
    assert store.cp["frontiers"] == before["frontiers"]
    assert store.cp["changesThrough"] == before["changesThrough"]


def test_backdated_insert_extends_implicit_domain_and_rewinds_bootstrap():
    source, store = Source(), Store()
    run(source, store, startTime=None)
    store.versioned = True
    store.cutoff = timestamp("2026-01-01T00:30:00Z")
    source.rows.insert(0, {"phenomenonTime": "2025-12-31T23:55:00Z", "result": 10})
    source.changes = [
        {
            "phenomenonTime": "2025-12-31T23:55:00Z",
            "systemTimeValidity": "2026-01-01T00:25:00Z/infinity",
        }
    ]
    run(source, store, startTime=None, bucketsToRecompute=1)
    assert store.cp["domainStart"] == "2025-12-31T23:55:00+00:00"
    earlier = store.rows["observations_10min", timestamp("2025-12-31T23:50:00Z")]
    assert earlier["sum_value"] == 10


def test_engine_flushes_full_batches_and_final_remainder():
    source, store = Source(), Store()
    store.cutoff = timestamp("2026-01-01T01:00:00Z")
    run(source, store, batch_size=4)
    ten_minute = [
        rows for table, rows in store.batches if table == "observations_10min"
    ]
    assert [len(rows) for rows in ten_minute] == [4, 2]
    assert len(store.rows) == 11  # six 10-minute buckets plus five larger intervals
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
        run(source, store, batch_size=2)
    assert len(store.rows) == 2 and not store.completed
    assert len(capsys.readouterr().out.splitlines()) == 2
    assert store.cp["frontiers"]["PT10M"] == "2026-01-01T00:20:00+00:00"
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
    individual_requests = len(source.requests)
    source.requests.clear()
    grouped = Store()
    grouped.cutoff = reference.cutoff
    run(source, grouped, batch_size=1000, boundaryMode=closure)
    assert grouped.rows == reference.rows
    assert len(source.requests) < individual_requests / 4
    assert grouped.cp == reference.cp


def test_seven_days_of_empty_ten_minute_buckets_need_two_group_reads():
    source, store = Source(), Store()
    source.rows = []
    store.cutoff = timestamp("2026-01-08T00:00:00Z")
    run(source, store, startTime="2026-01-01T00:00:00.000001Z")
    ten_minute = [
        rows for table, rows in store.batches if table == "observations_10min"
    ]
    assert sum(map(len, ten_minute)) == 1008
    assert (
        len(source.requests) == 8
    )  # two 10-minute groups, two Monday-aligned weeks, four other intervals
    assert all(row["count_numeric"] == 0 for rows in ten_minute for row in rows)
