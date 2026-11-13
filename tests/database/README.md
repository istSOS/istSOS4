# Continuous aggregate feasibility probe

The right-closed, fixed-UTC+1 aggregation proposal passed **58 checks, with zero
failures**, on PostgreSQL 18.3 / TimescaleDB **2.25.2**, on 5 November 2026.
This validates database feasibility on that version. It does not implement the
Aggregate API or establish the production QC bitmask policy.

## Reproduce

From the repository root, with Python 3 and Docker available:

```bash
docker pull timescale/timescaledb-ha:pg18-ts2.25
python3 tests/database/probe_continuous_aggregates.py \
  --report /tmp/istsos-cagg-probe.json
```

The default image matches `database/Dockerfile`. The tested image digest was
`sha256:416fe955f5d9aa42a5a1c273e63d6f6c3480faa9ca98171924ef09fa6315da5e`.
For an identical image, pass:

```bash
python3 tests/database/probe_continuous_aggregates.py \
  --image timescale/timescaledb-ha@sha256:416fe955f5d9aa42a5a1c273e63d6f6c3480faa9ca98171924ef09fa6315da5e
```

The script creates a uniquely named container with no published ports, no
network, and no existing volumes. It loads the repository's actual schema,
auth setup (disabled), and versioning triggers, with versioning enabled and
duplicates disabled. The container and its anonymous volumes are removed in
`finally`. Existing containers and istSOS databases are not used.

The captured run is in `continuous_aggregates-results.json`. A nonzero exit
status means a check failed or setup could not finish.

## What passed

The fixture starts with 2,588 observations in two Datastreams, spanning multiple
yearly chunks. It includes exact boundaries and timestamps one microsecond
before and after them, uneven sample counts, missing numeric values, a text
result, and deliberately excluded samples. Its independent Python oracle uses
calendar arithmetic in a fixed UTC+1 timezone, not SQL `time_bucket()`.

Each resolution is tested both directly over `sensorthings."Observation"` and
in this hierarchy:

```text
Observation -> 10 minutes -> hour -> day -> 7 days
                                       -> month -> year
```

All six resolutions (`PT10M`, `PT1H`, `P1D`, `P7D`, `P1M`, `P1Y`) matched the
oracle for total count, included count, sum, minimum, maximum and weighted mean:

- On initial materialization.
- After late boundary inserts, historical value corrections and filter
  membership changes.
- After a timestamp correction across chunks using delete-and-reinsert.
- After deleting the only observation in a bucket, including one whose removal
  empties every resolution up to an entire annual group.
- After a further late insertion and propagation through the hierarchy.

Additional checks cover Monday-aligned weeks, February in a leap year,
calendar months and years, and fixed UTC+1 on European DST transition dates.
The six bucket expressions also matched the oracle with connection timezones
set to Zurich, New York and Kathmandu. An incremental refresh was performed
under a Zurich connection timezone.

The actual versioning triggers archived six old rows after the main mutations.
The CAGGs matched current observations, without including those historical
versions. All refreshes used the default `force=false`.

## SQL convention validated

For every tested resolution, use the same offset expression:

```sql
time_bucket(
    INTERVAL '1 day', -- replace with the configured resolution
    "phenomenonTimeStart",
    INTERVAL '-1 hour 1 microsecond'
)
```

This reproduces right-closed logical intervals at PostgreSQL's microsecond
precision. It does not add a native `right_closed` option to TimescaleDB.
The offset also places daily, weekly and calendar boundaries at UTC+1.
Higher levels apply the same expression to the lower CAGG's `bucket` column.

The stored bucket timestamp is the **logical interval start plus 1 microsecond**.
Public interval labels should remove that microsecond; they should not remove
the UTC+1 alignment. Calculate the end in the fixed-offset local calendar:

```sql
bucket - INTERVAL '1 microsecond' AS logical_start,
(((bucket - INTERVAL '1 microsecond') AT TIME ZONE INTERVAL '1 hour')
    + INTERVAL '1 month') AT TIME ZONE INTERVAL '1 hour' AS logical_end
```

The probe checks both interval endpoints for every resolution under UTC,
Zurich and New York connection timezones. API serialization and self-links
remain outside this probe.

## Constraints demonstrated

1. **Refresh using internal bucket boundaries.** For a logical ten-minute
   interval `(10:00, 10:10]`, refreshing `[10:00, 10:10)` failed with
   `refresh window too small`. Refreshing from `10:00:00.000001` through
   `10:10:00.000001` materialized the expected sample. Broad refresh windows
   must similarly enclose the full shifted buckets, including annual ones.
   This agrees with TimescaleDB's rule that a refresh includes only whole
   buckets inside its window ([refresh documentation](https://www.tigerdata.com/docs/reference/timescaledb/continuous-aggregates/refresh_continuous_aggregate)).

2. **Historical corrections need a refresh covering their dates.** The probe
   executes a real seven-day refresh policy through `run_job()`. A corrected
   2024 bucket remains stale until the explicit historical refresh. Refreshing
   every level from fine to coarse then propagates the correction correctly.

3. **Calendar interval endpoints require fixed-local arithmetic.** For March
   2024, adding one month directly to the logical UTC start
   `2024-02-29T23:00:00Z` gives `2024-03-29T23:00:00Z`. The correct fixed-UTC+1
   end is `2024-03-31T23:00:00Z`. The local-calendar expression above passed
   for all six resolutions, including monthly and yearly endpoints.

4. **Cross-chunk timestamp UPDATE is rejected.** On this version, directly
   updating a timestamp into another chunk fails a chunk check constraint.
   The probe verifies that a transactional delete-and-reinsert, preserving
   the observation ID, works with the real history triggers and CAGG
   invalidation. This is a source-table constraint, separate from bucketing.

## Boundaries of this result

The eligibility predicate `resultType = 0`, numeric value present, and
`resultQuality = 3` is solely a controllable fixture for testing FILTER
invalidation. It is **not** an agreed production QC filter or bitmask mapping.
There is no QC propagation or coverage calculation implementation here.

This is a correctness probe on a small synthetic dataset, not a production
throughput or ingestion benchmark. It does not test concurrent writers,
compression/retention, crash recovery, automatic scheduling across hierarchy
levels, API authorization/pagination, or gap filling. Only TimescaleDB 2.25.2
was exercised. Production definitions and refresh orchestration remain to be
implemented after the QC policy is specified.

## Compatibility of the existing aggregation procedure's query

The 58 correctness checks above use simple grouped queries. They do not establish
that the complete calculation in `database/functions.sql` can be used as a CAGG.

`probe_cagg_query_compatibility.py` tests that separately:

```bash
python3 tests/database/probe_cagg_query_compatibility.py \
  --report /tmp/istsos-cagg-query-compatibility.json
```

It uses the same disposable database harness, loads the actual aggregation
procedures and helpers, and extracts the existing calculation from
`WITH normal_buckets` through `aggregated`, before the Observation upsert.
Procedure variables are replaced with concrete, typed configuration values.
The extracted query runs successfully as an ordinary SELECT, returning six
buckets for the fixture's explicit window. CAGG creation then rejects it with:

```text
ERROR: invalid continuous aggregate query
DETAIL: CTEs and subqueries are not supported by continuous aggregates.
```

The six compatibility assertions on TimescaleDB 2.25.2 are:

| Candidate | Ordinary SELECT | CAGG creation |
|---|---|---|
| Simple grouped Observation query | Accepted | Accepted |
| Existing calculation CTEs | Accepted | Rejected |
| Simple source CTE | Accepted | Rejected |
| Constant `generate_series(1, 1)` cross join | Accepted | Accepted |
| Availability-style `EXISTS` subquery | Accepted | Rejected |
| Table-returning function hiding the calculation | Accepted | Rejected: no visible hypertable |

In particular, a blanket claim that every set-returning function is unsupported
would be incorrect on this version: the constant series join was accepted.
That acceptance does not validate the current generated-bucket query or its
gap and availability semantics as a CAGG.

The captured results are in `cagg_query_compatibility-results.json`. These tests
establish that the existing calculation is not a drop-in CAGG definition and
that wrapping it in a function does not fix that. Rewriting parts as native
CAGGs with additional SQL for the remaining rules is a separate design task.
