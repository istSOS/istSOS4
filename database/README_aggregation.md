# Aggregation schema

`istsos_aggregation.sql` creates the `aggregation` schema used by the aggregation
service (`aggregation/`). It must run as superuser after `istsos_schema.sql`,
`istsos_auth.sql` and `istsos_schema_versioning.sql`; the database image loads it
automatically (`database/Dockerfile`). Existing databases are not migrated: rebuild
them from the image.

The script contains:

1. the `datastream_configuration` table (settings per Datastream);
2. six result tables, one per interval;
3. the `processing_state` table (latest run of each configuration) and its trigger;
4. the `safe_change_watermark()` function and two change-detection indexes.

## Table `datastream_configuration`

One row per aggregated Datastream. Read and written through the `/Aggregations` API.

### Identity and activation

- **`id`**: generated key (`BIGSERIAL`), used in `/Aggregations(id)`.
- **`datastream_id`**: source Datastream. Deleting the Datastream deletes its
  configuration (`ON DELETE CASCADE`).
- **`enabled`**: when `false` the configuration is not calculated; configuration and
  results are kept.
- **`created_at`**: creation time.
- **`updated_at`**: time of the last change (set by the API PATCH). If it changes while
  a calculation is running, the results of that run are discarded and recalculated
  with the new settings.

### Calculation mode (`mode`)

- **`sampled`** (default): regularly sampled series.
  - Requires `samplingFrequency` in the Datastream properties, otherwise it fails.
  - Knows how many values to expect: computes `expected_count` and `coverage`.
  - A bucket without values is **invalid**: statistics `NULL`, `-999.9` in the API.
- **`event`**: each observation is an event (e.g. a rain gauge tip).
  - Needs no `samplingFrequency`, so no coverage either.
  - A bucket without events is **valid and worth 0**.
  - A bucket is invalid (missing, `-999.9`) if all its events were rejected by QC.
  - An event with a `NULL` result (a tick without a value) counts as 1.
  - `min_coverage`, `min_observations` and `max_consecutive_missing` must be `NULL`.
  - May have a **`reference_datastream_id`**: a series of the same station (e.g. `Ta_`)
    that proves the station was active. An empty bucket is 0 only if the reference has an
    observation in the **`reference_max_age`** before the bucket end (default 1 hour);
    otherwise it is missing (a silent station is unknown, not dry). The evaluation stops
    at the latest observation of the source or the reference plus `reference_max_age`.
    Same rule as the old `empty_bucket_policy = zero_when_available` jobs.
    If the reference is deleted, the field becomes `NULL` (`ON DELETE SET NULL`).

### Processing period

- **`start_time`**: start of the history.
  - When `NULL`, the first observation of the series is used; a backdated insert moves
    the start back automatically.
  - When set, in `event` mode the buckets before the first event are created at 0.
- **`end_time`**: end of the aggregation; must be after `start_time`.

### Input quality (QC)

- **`ignore_quality`**: when `true` every observation counts, regardless of QC.
- **`min_input_quality`** (0–3, required): used when `ignore_quality = false`.
  - Human review wins: rejected = discarded, accepted = kept.
  - Otherwise the automatic level counts (3 all checks passed, 2 mixed, 1 all failed,
    0 not run): kept if level ≥ `min_input_quality`.
  - Observations without QC are kept only with threshold 0.
- **`result_quality_key`**: used when `resultQuality` is a JSON object; names the key
  holding the bitmask.

The aggregation does not run QC checks itself: they are run by the `qc` service, which
writes `resultQuality` on each observation.

### Bucket completeness (`sampled` only)

Each of these makes a bucket **invalid** when not met: the statistics are still stored,
but the result counts as missing data (`-999.9` in the API).

- **`min_coverage`** (0–1): `coverage = received values / expected values`.
  Expected = bucket duration / `samplingFrequency`, counted from `start_time` up to the
  evaluated point.
  *Example:* hourly bucket, 10-minute frequency, 5 values out of 6 → 0.83 → invalid with
  threshold 0.9.
- **`min_observations`**: minimum absolute number of valid values in the bucket.
- **`max_consecutive_missing`**: maximum number of consecutive missing expected samples,
  including gaps at the start and end of the bucket. Applies only to 10 minutes, hours
  and days: weeks, months and years are rolled up from days, and gaps across two days
  cannot be rebuilt.
  *Example:* 10-minute frequency, values at 00:10 and 00:50 → 3 missing (00:20, 00:30,
  00:40).

### Value transformation

- **`conversion_factor`** (default 1): multiplies every value before the statistics;
  multiplication only, no offset (°C → K is not possible).
  *Example:* rain gauge storing 1 per tip, 1 tip = 0.2 mm → factor 0.2, sums in mm.
- **`missing_values[]`** (default `[-999.9]`): values treated as missing data and
  excluded, checked before `conversion_factor`; `[]` = none. Never summed as numbers.

### Bucket alignment

- **`boundary_mode`** (default `right_closed`): decides which bucket each observation
  belongs to and which edge samples count in `expected_count`.
  - `right_closed` `(start, end]`: the value at 01:00 belongs to the 00:00–01:00 bucket
    (meteorological convention, labelled by the end).
  - `left_closed` `[start, end)`: the value at 01:00 belongs to the 01:00–02:00 bucket.
- **`utc_offset_minutes`** (−840…840, default `AGGREGATION_UTC_OFFSET_MINUTES` = 60):
  fixed offset used to align days, weeks (from Monday), months and years. With 60 the
  day runs from midnight UTC+1 to midnight UTC+1 all year, without daylight saving.

### Recalculation of late data

Only the touched buckets are recalculated:

- **with versioning**: the buckets containing observations inserted or updated since the
  last run (change detection through `lower(systemTimeValidity)`), of any age. The
  `phenomenonTime` of an observation is assumed never to change; deletions are not
  detected (use a manual recomputation);
- **without versioning**: the buckets overlapping the **`recompute_window`** (`INTERVAL`,
  e.g. `PT24H`) before the cutoff. When `NULL`, the Datastream `acquisitionFrequency` is
  used, else 24 hours. A bucket overlapping the window is recalculated in full.

A bucket still open (the cutoff falls inside it) is always recalculated on the next run.

In `event` mode with a reference, a late reference observation also recalculates the
buckets it proves active (those ending within `reference_max_age` after it): through the
reference changes with versioning, through the `recompute_window` without.

### Station activity (`event` only)

- **`reference_datastream_id`**: series of the same station proving it was active.
- **`reference_max_age`** (`INTERVAL`, `NULL` = 1 hour): an empty bucket is 0 only if the
  reference has an observation in `[end - reference_max_age, end]`, where `end` is the
  bucket end (or how far it is evaluated, for an open bucket); otherwise it is missing.
  Ignored without a reference.

### Constraints

PostgreSQL checks them on every `INSERT` and `UPDATE`: if one fails the row is not saved
and the error names the constraint. The pattern `X IS NULL OR condition` means "if X is
set, the condition must hold".

#### `unique_datastream_configuration_datastream_id`

```sql
UNIQUE ("datastream_id")
```

At most **one configuration per Datastream**. A second one is rejected (the API returns 409).

#### `datastream_configuration_period`

```sql
"start_time" IS NULL OR "end_time" IS NULL OR "start_time" < "end_time"
```

When both dates are set, the start must precede the end. When one is missing, the period
is open on that side.

| `start_time` | `end_time` | Result |
|---|---|---|
| NULL | NULL | OK (whole history) |
| 2024-11-25 | NULL | OK (from then on) |
| NULL | 2026-01-01 | OK (up to then) |
| 2024-11-25 | 2026-01-01 | OK |
| 2026-01-01 | 2024-11-25 | rejected |
| 2026-01-01 | 2026-01-01 | rejected (zero-length period) |

#### `datastream_configuration_event_completeness`

```sql
"mode" = 'sampled' OR ("min_coverage" IS NULL AND "min_observations" IS NULL AND "max_consecutive_missing" IS NULL)
```

The three completeness thresholds are allowed only in `sampled`. In `event` the number of
observations is not known in advance (an empty bucket is worth 0), so they must all be `NULL`.

| `mode` | thresholds | Result |
|---|---|---|
| sampled | any | OK |
| event | all NULL | OK |
| event | `min_coverage = 0.5` | rejected |

#### `datastream_configuration_reference`

```sql
"reference_datastream_id" IS NULL OR ("mode" = 'event' AND "reference_datastream_id" <> "datastream_id")
```

With a reference, the configuration must be `event` and the reference a different series:
it only decides the zero filling, and a series referencing itself would prove itself
active only where it already has events. The foreign key also requires the reference to exist.

| `mode` | reference | Result |
|---|---|---|
| any | NULL | OK |
| event | another Datastream | OK |
| sampled | another Datastream | rejected |
| event | itself | rejected |

#### `datastream_configuration_missing_values`

```sql
"missing_values" IS NULL OR (
    COALESCE(array_ndims("missing_values"), 1) = 1
    AND array_position("missing_values", NULL) IS NULL
    AND 'Infinity' > ALL ("missing_values")
    AND '-Infinity' < ALL ("missing_values")
)
```

When set, the list of missing-data markers must be a plain list of real numbers:

- `COALESCE(array_ndims(...), 1) = 1`: one dimension (no matrices). The empty array `{}`
  has `array_ndims` NULL, which `COALESCE` turns into 1: allowed, meaning "no markers".
- `array_position(..., NULL) IS NULL`: no `NULL` element.
- `'Infinity' > ALL (...)` and `'-Infinity' < ALL (...)`: all elements finite.

| `missing_values` | Result |
|---|---|
| NULL | OK (worker default: `-999.9`) |
| `{}` | OK (no markers) |
| `{-999.9, -9999}` | OK |
| `{-999.9, NULL}` | rejected |
| `{Infinity}` | rejected |
| `{{1,2},{3,4}}` | rejected |

Single-column rules (ranges of `min_input_quality`, `min_coverage`, `utc_offset_minutes`,
allowed values of `mode` and `boundary_mode`, positive `recompute_window`, non-blank
`result_quality_key`) are column `CHECK`s.

## Result tables

Six identical tables, one per interval: `observations_10min`, `observations_1hour`,
`observations_1day`, `observations_7days`, `observations_1month`, `observations_1year`.
The script creates them in a loop (`DO $$ ... FOREACH ... LOOP` + `EXECUTE format(...)`),
so the definition is written once. One row per Datastream and bucket.

**10min, 1hour and 1day** are calculated from the observations; **7days, 1month and 1year**
are rolled up from the saved daily rows: sums, minima, maxima and counts combine,
coverage and validity are recomputed from the summed counts, QC check pairs follow the
same rule as for observations. `max_consecutive_missing` applies only to 10min, 1hour
and 1day.

There is no foreign key to the configuration: deleting a configuration keeps its
results. Deleting the Datastream deletes them too.

### Bucket

- **`datastream_id`**: source series (foreign key to the Datastream, `ON DELETE CASCADE`).
- **`bucket_start`**: bucket start.
- **`bucket_end`**: bucket end. In the `/Aggregates` API the `phenomenonTime` is
  `bucket_start/bucket_end` and the time filter uses the end.
- **`evaluated_until`**: how far the bucket has been calculated. When lower than
  `bucket_end` the bucket is still open (current bucket, or cut by `end_time` or the
  reference) and may change in later runs.

### Counts

- **`count_total`**: observations received in the bucket.
- **`count_accepted`**: those that passed the QC filter (equal to `count_total` with
  `ignore_quality = true`).
- **`count_numeric`**: those accepted and valid numbers (not null, not in
  `missing_values`, finite). Statistics are calculated on these.
- **`expected_count`**: values expected from `samplingFrequency` (`sampled` only, `NULL`
  in `event`).
- **`coverage`**: `count_numeric / expected_count`. May exceed 1 when more values than
  expected arrive (duplicates, oversampling).

### Statistics

- **`sum_value`**, **`min_value`**, **`max_value`**, **`avg_value`**: calculated on the
  `count_numeric` values, after `conversion_factor`.
  - In `sampled`, a bucket without values has all statistics `NULL` (`-999.9` in the API).
  - In `event`, a bucket without events has all statistics at `0`.

### Quality

- **`result_quality`**: bucket quality bitmask, in 2-bit pairs: data (1 pair) +
  N automatic checks (N pairs) + human review (1 pair) + aggregation outcome (1 pair).
  - data: `11` valid, `01` valid empty event, `00` invalid;
  - each check: `10` if a contributing value failed it, `11` if all passed, else `00`;
  - human: always `00` (nobody reviewed the aggregate);
  - aggregation: `11` valid, `10` invalid.
- **`constraint_count`** (0–28): number of QC checks (`constraints`) of the Datastream at
  calculation time; tells where the pairs are in the bitmask.

### Traceability

- **`calculated_at`**: write time (`clock_timestamp()`, so different for each row even in
  the same batch). In the API it is the `resultTime`.
- **`configuration_updated_at`**: `updated_at` of the configuration used for the row, so
  results calculated with older settings are recognisable. The worker also uses it to
  discard a batch if the configuration changed during the calculation.

### Permissions

- `administrator` (worker and API): all privileges on the tables.
- `user`, `guest`, `sensor` (the roles the API switches to, `guest` for anonymous
  access): `SELECT` only on `datastream_configuration` and the six tables, for
  `/Datastreams(n)/Aggregates`. Granted only if the roles exist. `processing_state` is not
  readable by these roles.

### Constraints

#### Primary key

```sql
PRIMARY KEY ("datastream_id", "bucket_start")
```

One row per series and bucket. Lets the worker upsert
(`INSERT ... ON CONFLICT (datastream_id, bucket_start) DO UPDATE`): a recalculation
overwrites the row instead of duplicating it.

#### Finite times

Column checks on `bucket_start`, `bucket_end`, `evaluated_until`, `calculated_at`:

```sql
CHECK (isfinite("bucket_start"))
```

Rejects the special values `infinity` / `-infinity`.

#### `bucket_duration`

```sql
CHECK ("bucket_end" > "bucket_start")
```

A bucket has a positive duration.

#### `bucket_evaluated_until`

```sql
CHECK ("evaluated_until" BETWEEN "bucket_start" AND "bucket_end")
```

The evaluated point falls inside the bucket.

#### `bucket_counts`

```sql
CHECK ("count_accepted" BETWEEN 0 AND "count_total" AND "count_numeric" BETWEEN 0 AND "count_accepted")
```

Plus the column checks `"count_total" >= 0` and `"expected_count" >= 0`.

`0 ≤ count_numeric ≤ count_accepted ≤ count_total`: no more accepted than received, and
numeric values are a subset of the accepted ones.

| total | accepted | numeric | Result |
|---|---|---|---|
| 6 | 6 | 5 | OK |
| 6 | 7 | 5 | rejected |
| 6 | 4 | 5 | rejected |

#### `bucket_statistics`

```sql
CHECK (
    (count_numeric = 0 AND sum_value IS NULL AND min_value IS NULL AND max_value IS NULL AND avg_value IS NULL)
    OR
    (count_numeric = 0 AND sum_value = 0 AND min_value = 0 AND max_value = 0 AND avg_value = 0)
    OR
    (count_numeric > 0 AND sum_value IS NOT NULL AND min_value IS NOT NULL AND max_value IS NOT NULL AND avg_value IS NOT NULL)
)
```

Only three combinations are allowed, so half-filled rows cannot exist (e.g. a sum with a
`NULL` mean):

| Case | `count_numeric` | statistics | When |
|---|---|---|---|
| A | 0 | all `NULL` | `sampled` without values, `event` with all events rejected, or `event` without events and without a recent reference observation |
| B | 0 | all `0` | `event` without events, station active: no rain = 0 |
| C | > 0 | all present | normal bucket |

#### Finite statistics and `bucket_min_max`

```sql
"sum_value" DOUBLE PRECISION CHECK ("sum_value" > '-Infinity' AND "sum_value" < 'Infinity')  -- same for min, max, avg
CONSTRAINT bucket_min_max CHECK ("min_value" <= "max_value")
```

No infinite statistic, and the minimum does not exceed the maximum. A `NULL` always
passes (case A above).

#### `bucket_coverage`

```sql
CHECK (CASE WHEN expected_count IS NULL OR expected_count = 0
    THEN coverage IS NULL
    ELSE coverage IS NOT NULL AND coverage >= 0 AND coverage < 'Infinity'
    END)
```

| `expected_count` | `coverage` | Result |
|---|---|---|
| NULL (`event`) | NULL | OK |
| 0 | NULL | OK (no division by 0) |
| 0 | 0.5 | rejected |
| 6 | 0.83 | OK |
| 6 | 1.17 | OK (more values than expected) |
| 6 | NULL | rejected |

#### `bucket_result_quality`

```sql
CHECK ("result_quality" >> (2 * ("constraint_count" + 3)) = 0)
```

Plus the column checks `"result_quality" >= 0` and `"constraint_count" BETWEEN 0 AND 28`.

The bitmask has `constraint_count + 3` pairs of 2 bits (data + N checks + human +
aggregation). Shifting right by `2 · (N + 3)` bits must leave 0: no bit set beyond the
layout. With N ≤ 28 at most 62 bits are needed, which fit in a positive `BIGINT`.

*Example with N = 1:* 4 pairs, 8 bits. `result_quality = 0b11_00_11_11` (207) is accepted;
a value with the ninth bit set (≥ 256) is rejected.

## Table `processing_state`

One row per configuration with the state of the **latest** run. Written by the worker
only; read-only through the API at `GET /Aggregations(id)/ProcessingState`
(administrator only). Not a history: each run overwrites the previous one.

The row is created automatically by the `initialize_processing_state` trigger for every
new configuration (status `never_run`) and deleted with the configuration
(`ON DELETE CASCADE`). Results are kept.

### Columns

- **`aggregation_id`**: primary key and foreign key to `datastream_configuration(id)`.
- **`status`**: run state.

  | Value | When |
  |---|---|
  | `never_run` | just created, never run |
  | `running` | calculation in progress |
  | `success` | finished without errors |
  | `failed` | API or calculation error |
  | `interrupted` | the service stopped mid-run (stop, crash, lost connection); on restart the worker marks every row left `running` this way |

- **`run_id`**: UUID of the current run. Every update filters on `run_id`, so a run that
  lost ownership cannot overwrite the state of a newer one.
- **`last_started_at`** / **`last_finished_at`**: start and end of the latest run (end
  `NULL` while `running`).
- **`last_successful_at`**: latest successful run; kept after later failures.
- **`last_error`**: JSON `{"step": ..., "message": ...}` with the latest error; `NULL`
  after a success.
- **`consecutive_failures`**: failed or interrupted runs in a row; back to 0 on success.
- **`checkpoint`**: JSON with where to resume, saved **in the same transaction** as each
  batch of results (if the write fails, the checkpoint does not advance either).

  | Key | Meaning |
  |---|---|
  | `frontiers` | `{interval: timestamp}` for 10min, 1h and 1d: how far the calculation got (an open bucket does not advance it) |
  | `domainStart` | start of the history used |
  | `layout` | hash of the Datastream QC `constraints`: if they change, the worker stops and asks for a manual recalculation |
  | `changesThrough` | system time up to which changes have been scanned (versioning) |

- **`updated_at`**: last update of the row.

### Permissions

`administrator` only. Unlike the result tables, `user`, `guest` and `sensor` cannot read it.

### Constraints

#### Column checks

- `status` only from its list.
- `last_started_at`, `last_finished_at`, `last_successful_at` finite (no `infinity`).
- `last_error` and `checkpoint` must be JSON objects.
- `consecutive_failures >= 0`.

#### `processing_state_run_order`

```sql
CHECK ("last_finished_at" >= "last_started_at")
```

The end cannot precede the start.

#### `processing_state_status`

```sql
CHECK (
    ("status" = 'never_run' AND "run_id" IS NULL AND "last_started_at" IS NULL AND "last_finished_at" IS NULL)
    OR ("status" = 'running' AND "run_id" IS NOT NULL AND "last_started_at" IS NOT NULL AND "last_finished_at" IS NULL)
    OR ("status" IN ('success', 'failed', 'interrupted') AND "run_id" IS NOT NULL
        AND "last_started_at" IS NOT NULL AND "last_finished_at" IS NOT NULL)
)
```

The status must match the run and its times:

| `status` | `run_id` | `last_started_at` | `last_finished_at` | Result |
|---|---|---|---|---|
| `never_run` | NULL | NULL | NULL | OK |
| `running` | set | set | NULL | OK |
| `success` / `failed` / `interrupted` | set | set | set | OK |
| `running` | set | set | set | rejected (running but finished) |
| `success` | NULL | NULL | NULL | rejected (success without a run) |

## Watermark and indexes

### Function `safe_change_watermark()`

Returns the worker's **cutoff time**: "process everything committed up to this instant".
With versioning the worker uses it as `$as_of` when reading (a stable snapshot of the
database), as the end of the change-scan window (`changesThrough`) and as the upper limit
of the buckets to calculate. Without versioning the worker uses the current second and
does not call it.

**Why `now()` is not enough:** a transaction started at 10:00 inserts an observation whose
`systemTimeValidity` starts at 10:00, but commits at 10:05. A run at 10:03 with cutoff
10:03 does not see it (not committed yet); the next run scans changes after 10:03 and
misses it too (its version starts at 10:00). It would be lost. So the cutoff stays
**behind the start of the oldest open transaction**.

How it works:

- **`SECURITY DEFINER`**: runs with the privileges of its owner (the superuser applying
  the script), because the start of other users' transactions in `pg_stat_activity` is
  visible only to superusers or members of `pg_read_all_stats`. If the owner lacks them,
  the function fails rather than returning a wrong value.
- **`SET search_path = pg_catalog`**: a privileged function must not resolve names with
  the caller's `search_path`.
- **Prepared transactions** (`PREPARE TRANSACTION`) have no reliable start time. While one
  exists it returns `NULL` and the worker waits until it is resolved. istSOS does not
  use them.
- **Calculation**: the minimum of the current time and the start of the oldest open
  transaction in the database (excluding the current session), truncated to the second
  (precision of `$as_of`) minus 1 second.
- **Permissions**: execution revoked from `PUBLIC`, granted only to `administrator` (the worker).

**Warning:** a session left `idle in transaction` (e.g. a DBeaver tab with an open
`BEGIN`) freezes the watermark at its start time: aggregations stop advancing until that
transaction is closed.

### Change-detection indexes

Created only when versioning is installed (`systemTimeValidity` column); without
versioning the block does nothing. It serves the change scan the worker runs through the
API (`$filter` on `lower(systemTimeValidity)`), one series at a time.

| Index | Table | Columns | Finds |
|---|---|---|---|
| `idx_observation_datastream_system_time` | `sensorthings."Observation"` | `datastream_id`, `lower(systemTimeValidity)` | versions **opened** in the window: inserts and updates |
