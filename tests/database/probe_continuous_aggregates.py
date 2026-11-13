#!/usr/bin/env python3
"""Disposable TimescaleDB feasibility probe; no running istSOS database is used.

Uses the repository's real schema and versioning triggers. Docker is the only
external dependency. QC eligibility below is deliberately a test fixture, not
the proposed production bitmask policy. See README.md in this directory.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import subprocess
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
LOCAL = timezone(timedelta(hours=1))
EPSILON = timedelta(microseconds=1)
INTERVALS = {
    "10min": ("10 minutes", None),
    "hour": ("1 hour", "10min"),
    "day": ("1 day", "hour"),
    "week": ("7 days", "day"),
    "month": ("1 month", "day"),
    "year": ("1 year", "month"),
}
OFFSET = "INTERVAL '-1 hour 1 microsecond'"
SOURCE = 'sensorthings."Observation"'
# A controllable membership predicate for invalidation tests only.
ELIGIBLE = '"resultType" = 0 AND "resultNumber" IS NOT NULL AND "resultQuality" = \'3\'::jsonb'


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def bucket_start(value, resolution):
    """Independent oracle: local calendar arithmetic, no SQL time_bucket()."""
    local = (value - EPSILON).astimezone(LOCAL)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if resolution == "10min":
        start = local.replace(minute=local.minute // 10 * 10, second=0, microsecond=0)
    elif resolution == "hour":
        start = local.replace(minute=0, second=0, microsecond=0)
    elif resolution == "day":
        start = midnight
    elif resolution == "week":
        start = midnight - timedelta(days=local.weekday())
    elif resolution == "month":
        start = midnight.replace(day=1)
    else:
        start = midnight.replace(month=1, day=1)
    return start.astimezone(UTC) + EPSILON


def bucket_end(internal_start, resolution):
    local = (internal_start - EPSILON).astimezone(LOCAL)
    if resolution == "month":
        end = local.replace(year=local.year + (local.month == 12), month=local.month % 12 + 1)
    elif resolution == "year":
        end = local.replace(year=local.year + 1)
    else:
        end = local + {"10min": timedelta(minutes=10), "hour": timedelta(hours=1),
                       "day": timedelta(days=1), "week": timedelta(days=7)}[resolution]
    return end.astimezone(UTC)


class Probe:
    def __init__(self, image):
        self.image = image
        self.container = "istsos-cagg-probe-" + uuid.uuid4().hex[:10]
        self.results = []
        self.views = []
        self.rows = {}
        self.next_id = 1
        self.started = False
        self.report = {"image": image, "results": self.results}

    def docker(self, *args, text=None, timeout=180):
        result = subprocess.run(
            ["docker", *args], input=text, text=True, capture_output=True, timeout=timeout
        )
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result.stdout.strip()

    def sql(self, sql):
        return self.docker(
            "exec", "-i", self.container, "psql", "-X", "-qAt",
            "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-U", "postgres", "-d", "probe",
            text="SET statement_timeout = '15s'; SET TIME ZONE 'UTC';\n" + sql,
        )

    def record(self, name, operation):
        started = time.monotonic()
        try:
            detail = operation()
            result = {"name": name, "status": "pass", "detail": detail}
        except Exception as error:
            result = {"name": name, "status": "fail", "detail": str(error)}
        result["seconds"] = round(time.monotonic() - started, 3)
        self.results.append(result)
        print(f"{result['status'].upper()}: {name}: {result['detail']}", flush=True)
        return result["status"] == "pass"

    def start(self):
        # No published ports, no existing volumes, and only disposable data.
        self.docker(
            "run", "-d", "--name", self.container, "--network", "none",
            "--memory", "2g", "--shm-size", "256m",
            "-e", "POSTGRES_PASSWORD=disposable-test-only",
            "-e", "POSTGRES_DB=probe", self.image,
            "postgres", "-c", "custom.versioning=1", "-c", "custom.network=0",
            "-c", "custom.authorization=0", "-c", "custom.duplicates=0",
            "-c", "custom.epsg=4326", "-c", "custom.user=probe_admin",
            "-c", "custom.password=disposable-test-only", "-c", "max_worker_processes=16",
        )
        self.started = True
        for _ in range(90):
            try:
                if self.sql("SELECT 1;") == "1":
                    break
            except RuntimeError:
                time.sleep(1)
        else:
            raise RuntimeError("Database did not become ready")
        # Load the actual files, not a simplified imitation of Observation.
        for filename in ("istsos_schema.sql", "istsos_auth.sql", "istsos_schema_versioning.sql"):
            self.sql((ROOT / "database" / filename).read_text())
        self.report["versions"] = json.loads(self.sql("""
            SELECT json_build_object('postgres', version(), 'timescaledb',
                (SELECT extversion FROM pg_extension WHERE extname='timescaledb'));
        """))
        print(json.dumps(self.report["versions"]), flush=True)
        self.sql("""
            CREATE SCHEMA cagg_probe;
            INSERT INTO sensorthings."Commit" (id, author, message, "actionType")
                VALUES (1, 'probe', 'Disposable aggregation test', 'CREATE');
            INSERT INTO sensorthings."Thing" (id, name, description, commit_id)
                VALUES (1, 'probe', 'probe', 1);
            INSERT INTO sensorthings."Sensor"
                (id, name, description, "encodingType", metadata, commit_id)
                VALUES (1, 'probe', 'probe', 'text/plain', 'probe', 1);
            INSERT INTO sensorthings."ObservedProperty"
                (id, name, definition, description, commit_id)
                VALUES (1, 'probe', 'probe', 'probe', 1);
            INSERT INTO sensorthings."FeaturesOfInterest"
                (id, name, description, "encodingType", feature, commit_id)
                VALUES (1, 'probe', 'probe', 'application/geo+json',
                    ST_SetSRID(ST_MakePoint(8, 46), 4326), 1);
            INSERT INTO sensorthings."Datastream"
                (id, name, description, "unitOfMeasurement", "observationType",
                 thing_id, sensor_id, observedproperty_id, commit_id)
                SELECT id, 'probe-' || id, 'probe', '{}'::jsonb, 'probe', 1, 1, 1, 1
                FROM generate_series(1, 2) id;
        """)

    def add(self, time_value, value, stream=1, quality=3, result_type=0):
        row_id = self.next_id
        self.next_id += 1
        self.rows[row_id] = {
            "id": row_id, "time": time_value, "value": value,
            "stream": stream, "quality": quality, "type": result_type,
        }
        return row_id

    def insert(self, ids):
        values = []
        for row_id in ids:
            row = self.rows[row_id]
            value = "NULL" if row["value"] is None else str(row["value"])
            values.append(
                f"({row_id}, '{iso(row['time'])}', '{iso(row['time'])}', "
                f"{row['type']}, {value}, '{row['quality']}'::jsonb, {row['stream']}, 1, 1)"
            )
        self.sql(f"""INSERT INTO {SOURCE}
            (id, "phenomenonTimeStart", "phenomenonTimeEnd", "resultType",
             "resultNumber", "resultQuality", datastream_id, featuresofinterest_id, commit_id)
            VALUES {','.join(values)};""")

    def seed(self):
        # Exact boundaries and +/- 1 microsecond: hour, day, Monday, leap day,
        # month, year, and both European DST dates (offset remains fixed).
        edges = [
            "2023-01-01T00:00:00+01:00", "2023-12-31T00:00:00+01:00",
            "2024-01-01T00:00:00+01:00", "2024-02-01T00:00:00+01:00",
            "2024-02-29T00:00:00+01:00", "2024-03-01T00:00:00+01:00",
            "2024-03-31T00:00:00+01:00", "2024-04-01T00:00:00+01:00",
            "2024-10-27T00:00:00+01:00", "2024-10-28T00:00:00+01:00",
            "2025-01-01T00:00:00+01:00", "2026-11-05T10:00:00+01:00",
            "2026-11-05T10:10:00+01:00", "2026-11-05T11:00:00+01:00",
        ]
        for stream in (1, 2):
            for value in edges:
                for delta in (-EPSILON, timedelta(0), EPSILON):
                    self.add(timestamp(value) + delta, self.next_id % 97, stream)
        # Unequal sample counts ensure mean-of-means would fail the oracle.
        rng = random.Random(1729)
        base = timestamp("2023-01-01T00:00:00Z")
        used = {(r["stream"], r["time"]) for r in self.rows.values()}
        for _ in range(2500):
            value = base + timedelta(seconds=rng.randrange(4 * 365 * 86400), microseconds=rng.randrange(1000000))
            stream = rng.choice((1, 2))
            if (stream, value) in used:
                continue
            used.add((stream, value))
            self.add(value, rng.randrange(-20, 80), stream, quality=rng.choice((0, 3, 3, 3)))
        self.add(timestamp("2024-02-15T12:34:00Z"), None)
        self.add(timestamp("2024-02-15T12:35:00Z"), None, result_type=3)
        # A bucket containing exclusively ineligible samples.
        self.add(timestamp("2024-02-16T12:34:00Z"), 12, quality=0)
        # Its deletion will empty every resolution, including an entire year.
        self.isolated_id = self.add(timestamp("2027-06-15T12:34:00Z"), 91, stream=2)
        self.insert(list(self.rows))
        return f"{len(self.rows)} observations, two Datastreams, multiple yearly chunks"

    def create_view(self, kind, resolution):
        interval, parent = INTERVALS[resolution]
        name = f"cagg_probe.{kind}_{resolution}"
        rollup = kind == "hier" and parent is not None
        source = f"cagg_probe.hier_{parent}" if rollup else SOURCE
        time_column = "bucket" if rollup else '"phenomenonTimeStart"'
        stats = (
            "sum(n_total)::bigint AS n_total, sum(n_valid)::bigint AS n_valid, "
            "sum(sum_value) AS sum_value, min(min_value) AS min_value, max(max_value) AS max_value"
            if rollup else
            f"count(*) AS n_total, count(*) FILTER (WHERE {ELIGIBLE}) AS n_valid, "
            f'SUM("resultNumber") FILTER (WHERE {ELIGIBLE}) AS sum_value, '
            f'MIN("resultNumber") FILTER (WHERE {ELIGIBLE}) AS min_value, '
            f'MAX("resultNumber") FILTER (WHERE {ELIGIBLE}) AS max_value'
        )
        self.sql(f"""
            CREATE MATERIALIZED VIEW {name}
            WITH (timescaledb.continuous, timescaledb.materialized_only=true)
            AS SELECT datastream_id,
                time_bucket(INTERVAL '{interval}', {time_column}, {OFFSET}) AS bucket,
                {stats}
            FROM {source} GROUP BY datastream_id, 2 WITH NO DATA;
        """)
        self.views.append((kind, resolution))
        return f"created over {source}"

    def refresh(self, kinds=None, start="2021-01-01T00:00:00Z", end="2028-01-01T00:00:00Z", zone="UTC"):
        views = [(kind, resolution) for kind, resolution in self.views if kinds is None or kind in kinds]
        for kind, resolution in views:
            self.sql(f"SET TIME ZONE '{zone}'; CALL refresh_continuous_aggregate('cagg_probe.{kind}_{resolution}', "
                     f"'{start}'::timestamptz, '{end}'::timestamptz);")
        return f"refreshed {len(views)} CAGGs, fine to coarse, force=false"

    def expected(self, resolution):
        grouped = {}
        for row in self.rows.values():
            key = (row["stream"], bucket_start(row["time"], resolution))
            stats = grouped.setdefault(key, [0, 0, None, None, None])
            stats[0] += 1
            if row["type"] != 0 or row["value"] is None or row["quality"] != 3:
                continue
            value = row["value"]
            stats[1] += 1
            stats[2] = value if stats[2] is None else stats[2] + value
            stats[3] = value if stats[3] is None else min(stats[3], value)
            stats[4] = value if stats[4] is None else max(stats[4], value)
        return grouped

    def check(self, kind, resolution):
        raw = self.sql(f"""
            SELECT COALESCE(json_agg(t), '[]') FROM (
                SELECT datastream_id, bucket, n_total, n_valid, sum_value, min_value, max_value,
                    sum_value / NULLIF(n_valid, 0) AS avg_value
                FROM cagg_probe.{kind}_{resolution} ORDER BY datastream_id, bucket
            ) t;
        """)
        actual = {}
        for row in json.loads(raw):
            key = (row["datastream_id"], timestamp(row["bucket"]))
            actual[key] = [row[column] for column in ("n_total", "n_valid", "sum_value", "min_value", "max_value")]
            expected_avg = row["sum_value"] / row["n_valid"] if row["n_valid"] else None
            if row["avg_value"] != expected_avg:
                raise AssertionError(f"Incorrect weighted average at {key}")
        expected = self.expected(resolution)
        if actual != expected:
            mismatches = [
                {"key": str(key), "actual": actual.get(key), "expected": expected.get(key)}
                for key in actual.keys() | expected.keys() if actual.get(key) != expected.get(key)
            ]
            raise AssertionError(f"{len(mismatches)} bucket mismatches: {mismatches[:3]}")
        return f"{len(actual)} buckets match independent right-closed UTC+1 oracle (counts/sum/min/max/avg)"

    def check_all(self, stage):
        for kind, resolution in self.views:
            self.record(f"{stage}: {kind}_{resolution}", lambda k=kind, r=resolution: self.check(k, r))

    def mutate(self):
        # Late arrivals, including exact year/month/day/10-minute boundaries.
        new_ids = [self.add(timestamp(value), 123) for value in (
            "2023-02-01T00:00:00+01:00", "2024-05-06T00:00:00+01:00",
            "2024-05-06T00:10:00+01:00",
        )]
        self.insert(new_ids)
        # Change a historical value at an exact boundary.
        target = next(r for r in self.rows.values() if r["time"] == timestamp("2024-03-01T00:00:00+01:00") and r["stream"] == 1)
        self.sql(f'UPDATE {SOURCE} SET "resultNumber"=1001 WHERE id={target["id"]};')
        target["value"] = 1001
        # FILTER membership changes without modifying the time coordinate.
        admitted = next(r for r in self.rows.values() if r["quality"] == 0 and r["value"] is not None)
        excluded = next(r for r in self.rows.values() if r["quality"] == 3 and r["id"] != target["id"])
        self.sql(f'UPDATE {SOURCE} SET "resultQuality"=\'3\'::jsonb WHERE id={admitted["id"]};')
        admitted["quality"] = 3
        self.sql(f'UPDATE {SOURCE} SET "resultQuality"=\'0\'::jsonb WHERE id={excluded["id"]};')
        excluded["quality"] = 0
        # Timestamp correction moves a row across month AND year/chunk boundaries.
        moved = next(r for r in self.rows.values() if r["time"] == timestamp("2024-01-01T00:00:00+01:00") and r["stream"] == 1)
        new_time = timestamp("2025-01-01T00:01:23+01:00")
        try:
            self.sql(f'UPDATE {SOURCE} SET "phenomenonTimeStart"=\'{iso(new_time)}\', "phenomenonTimeEnd"=\'{iso(new_time)}\' WHERE id={moved["id"]};')
        except RuntimeError as error:
            if "violates check constraint" not in str(error):
                raise
            self.report["cross_chunk_update"] = {
                "status": "rejected_by_timescaledb", "error": str(error),
                "tested_alternative": "transactional DELETE followed by INSERT, preserving id",
            }
            # The failed UPDATE and its history insertion were rolled back.
            # Use a single transaction for the supported correction path.
            moved["time"] = new_time
            row = moved
            self.sql(f"""BEGIN;
                DELETE FROM {SOURCE} WHERE id={row['id']};
                INSERT INTO {SOURCE} (id, "phenomenonTimeStart", "phenomenonTimeEnd",
                    "resultType", "resultNumber", "resultQuality", datastream_id,
                    featuresofinterest_id, commit_id)
                VALUES ({row['id']}, '{iso(new_time)}', '{iso(new_time)}', 0,
                    {row['value']}, '{row['quality']}'::jsonb, {row['stream']}, 1, 1);
                COMMIT;""")
        else:
            moved["time"] = new_time
            self.report["cross_chunk_update"] = {"status": "accepted"}
        # Delete the only row in a particular 10-minute bucket.
        counts = self.expected("10min")
        deleted = next(r for r in self.rows.values() if counts[(r["stream"], bucket_start(r["time"], "10min"))][0] == 1 and r["quality"] == 3 and r["value"] is not None and r["id"] not in (admitted["id"], moved["id"], self.isolated_id))
        self.sql(f'DELETE FROM {SOURCE} WHERE id={deleted["id"]};')
        del self.rows[deleted["id"]]
        self.sql(f'DELETE FROM {SOURCE} WHERE id={self.isolated_id};')
        del self.rows[self.isolated_id]
        return "late insert, boundary-value update, eligibility changes, cross-chunk delete/reinsert, deletion to empty at all resolutions"

    def history(self):
        counts = json.loads(self.sql(f"""SELECT json_build_object(
            'current', (SELECT count(*) FROM {SOURCE}),
            'history', (SELECT count(*) FROM sensorthings_history."Observation"),
            'combined', (SELECT count(*) FROM sensorthings."Observation_traveltime"));"""))
        if counts["current"] != len(self.rows) or counts["history"] < 5:
            raise AssertionError(counts)
        if counts["combined"] != counts["current"] + counts["history"]:
            raise AssertionError(counts)
        return counts

    def recent_policy(self):
        # A policy cannot repair old invalidations outside its lookback window.
        # Exercise the actual background-job procedure synchronously.
        name = "cagg_probe.direct_10min"
        job_id = int(self.sql(f"""SELECT add_continuous_aggregate_policy('{name}',
            start_offset => INTERVAL '7 days', end_offset => INTERVAL '10 minutes',
            schedule_interval => INTERVAL '1 hour');"""))
        self.sql(f"SELECT alter_job({job_id}, scheduled => false); CALL run_job({job_id});")
        bucket = bucket_start(timestamp("2024-03-01T00:00:00+01:00"), "10min")
        actual = float(self.sql(f"SELECT sum_value FROM {name} WHERE datastream_id=1 AND bucket='{iso(bucket)}';"))
        expected = self.expected("10min")[(1, bucket)][2]
        if actual == expected:
            raise AssertionError("Recent-only policy unexpectedly refreshed the historical bucket")
        self.sql(f"SELECT remove_continuous_aggregate_policy('{name}');")
        return "7-day policy leaves a 2024 correction stale; explicit historical refresh is necessary"

    def refresh_boundary(self):
        # The human-readable right-closed interval differs from the internal
        # shifted CAGG bucket by one microsecond at BOTH ends.
        start = timestamp("2024-02-10T10:00:00Z")
        end = start + timedelta(minutes=10)
        row_id = self.add(end, 777)
        self.insert([row_id])
        name = "cagg_probe.direct_10min"
        try:
            self.sql(f"CALL refresh_continuous_aggregate('{name}', '{iso(start)}', '{iso(end)}');")
        except RuntimeError as error:
            if "refresh window too small" not in str(error):
                raise
            unshifted_error = "refresh window too small"
        else:
            unshifted_error = None
        query = f"SELECT COALESCE(sum(n_total),0) FROM {name} WHERE datastream_id=1 AND bucket='{iso(start + EPSILON)}';"
        unshifted_count = int(self.sql(query))
        self.sql(f"CALL refresh_continuous_aggregate('{name}', '{iso(start + EPSILON)}', '{iso(end + EPSILON)}');")
        shifted_count = int(self.sql(query))
        expected = self.expected("10min")[(1, start + EPSILON)][0]
        if unshifted_count == expected or shifted_count != expected:
            raise AssertionError({"unshifted": unshifted_count, "shifted": shifted_count, "expected": expected})
        return {"unshifted_window_error": unshifted_error,
                "unshifted_window_count": unshifted_count, "shifted_window_count": shifted_count,
                "lesson": "refresh must use internal boundaries (logical boundaries +1us)"}

    def session_timezones(self):
        # CAGGs were created under UTC; reads and incremental refresh must not
        # depend on the connection's TimeZone setting.
        for zone in ("Europe/Zurich", "America/New_York", "Asia/Kathmandu"):
            for resolution, (interval, _) in INTERVALS.items():
                actual = json.loads(self.sql(f"""SET TIME ZONE '{zone}';
                    SELECT json_agg(t) FROM (
                        SELECT datastream_id,
                            time_bucket(INTERVAL '{interval}', "phenomenonTimeStart", {OFFSET}) AS bucket,
                            count(*) AS n_total
                        FROM {SOURCE} GROUP BY datastream_id, 2
                    ) t;"""))
                grouped = {(r["datastream_id"], timestamp(r["bucket"])): r["n_total"] for r in actual}
                expected = {key: stats[0] for key, stats in self.expected(resolution).items()}
                if grouped != expected:
                    raise AssertionError(f"Bucketing changed for {resolution} under {zone}")
        return "all six time_bucket expressions match oracle under Zurich, New York and Kathmandu sessions"

    def interval_labels(self):
        # Adding a month to a UTC bucket start is not local-calendar arithmetic:
        # March starts Feb 29 at 23:00Z in leap-year 2024.
        march_start = timestamp("2024-03-01T00:00:00+01:00") + EPSILON
        naive = timestamp(self.sql(f"SELECT ('{iso(march_start)}'::timestamptz + INTERVAL '1 month' - INTERVAL '1 microsecond')::text;"))
        correct = bucket_end(march_start, "month")
        if naive == correct:
            raise AssertionError("Fixture failed to expose naive UTC calendar addition")
        for zone in ("UTC", "Europe/Zurich", "America/New_York"):
            for resolution, (interval, _) in INTERVALS.items():
                rows = json.loads(self.sql(f"""SET TIME ZONE '{zone}';
                    SELECT json_agg(t) FROM (
                        SELECT DISTINCT bucket,
                            bucket - INTERVAL '1 microsecond' AS logical_start,
                            ((bucket - INTERVAL '1 microsecond') AT TIME ZONE INTERVAL '1 hour'
                                + INTERVAL '{interval}') AT TIME ZONE INTERVAL '1 hour' AS logical_end
                        FROM cagg_probe.direct_{resolution}
                    ) t;"""))
                for row in rows:
                    start = timestamp(row["bucket"])
                    if timestamp(row["logical_start"]) != start - EPSILON or timestamp(row["logical_end"]) != bucket_end(start, resolution):
                        raise AssertionError(f"Incorrect interval label in {zone}: {row}")
        return {"naive_UTC_March_end": iso(naive), "correct_fixed_UTC1_March_end": iso(correct),
                "verified": "all six interval labels under UTC, Zurich and New York sessions"}

    def run(self):
        self.start()
        self.record("seed actual versioned Observation hypertable", self.seed)
        for kind in ("direct", "hier"):
            for resolution in INTERVALS:
                self.record(f"create {kind}_{resolution}", lambda k=kind, r=resolution: self.create_view(k, r))
        self.record("initial full refresh", self.refresh)
        self.check_all("initial")
        self.record("source mutations with actual history triggers", self.mutate)
        self.record("versioning keeps old values outside current source", self.history)
        self.record("recent policy versus historical correction", self.recent_policy)
        self.record("incremental historical refresh under Europe/Zurich session", lambda: self.refresh(zone="Europe/Zurich"))
        self.check_all("after mutations")
        self.record("session timezone independence", self.session_timezones)
        self.record("interval labels use fixed-local calendar arithmetic", self.interval_labels)
        self.record("refresh boundary alignment regression", self.refresh_boundary)
        self.record("final propagation through hierarchy", self.refresh)
        self.check_all("final")
        self.report["observation_count"] = len(self.rows)
        self.report["summary"] = {status: sum(r["status"] == status for r in self.results) for status in ("pass", "fail")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="timescale/timescaledb-ha:pg18-ts2.25")
    parser.add_argument("--report", type=Path, default=Path("/tmp/istsos-cagg-probe.json"))
    args = parser.parse_args()
    probe = Probe(args.image)
    try:
        probe.run()
    finally:
        if probe.started:
            probe.docker("rm", "-fv", probe.container)
        args.report.write_text(json.dumps(probe.report, indent=2) + "\n")
        print(f"Report: {args.report}", flush=True)
    return int(any(result["status"] == "fail" for result in probe.results))


if __name__ == "__main__":
    raise SystemExit(main())
