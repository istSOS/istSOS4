# Aggregation service

The aggregation service calculates, for each configured Datastream, statistics over six
fixed intervals: 10 minutes, 1 hour, 1 day, 7 days (from Monday), 1 month and 1 year.
For each bucket it stores count, sum, minimum, maximum, mean, coverage and a quality
bitmask in the `aggregation` schema, readable through `GET /Datastreams(id)/Aggregates`.

- 10-minute, hourly and daily buckets are calculated from the source observations.
- Weekly, monthly and yearly buckets are rolled up from the saved daily buckets.
- Source observations are never modified.

The database schema (configuration, result tables, run state, watermark) is described in
[database/README_aggregation.md](../database/README_aggregation.md). Configurations are
managed through the `/Aggregations` API.

## How it works

The service runs on a cron schedule. Each cycle:

1. reads the enabled configurations from `/Aggregations` (API);
2. for each configuration, one at a time:
   1. marks it `running` in `aggregation.processing_state` (database);
   2. reads the source Datastream (`samplingFrequency`, QC `constraints`) and logs any
      metadata warnings;
   3. computes the **cutoff**: the safe system-time watermark with versioning, else the
      current second; capped by `endTime` and, in event mode with a reference, by the latest
      observation of the source or the reference plus `referenceMaxAge`;
   4. finds the **buckets to calculate** for 10 minutes, hours and days:
      - the bootstrap run from the stored cursor up to the bucket containing the cutoff;
      - with versioning, every bucket containing an observation inserted or updated
        since the last scan;
      - without versioning, every bucket overlapping the look-back window before the
        cutoff (`recomputeWindow`, else the Datastream `acquisitionFrequency`, else 24 hours);
   5. reads the source observations **once** for those buckets, in windows of at most
      30 days, and adds each observation to the open bucket of every interval;
   6. writes the results in batches (COPY + upsert) together with the checkpoint;
   7. rolls up the weeks, months and years containing a recalculated day from the saved
      daily rows;
   8. marks the run `success` (or `failed` / `interrupted`).

The service uses two channels: the **API** for configurations, Datastreams, observations
and change detection, and the **database** for run state, ownership lock, watermark and
result writes.

## Start in development

Set these in the repository `.env`:

```dotenv
AGGREGATION_ENABLED=1
AGGREGATION_SCHEDULE="*/10 * * * *"
AGGREGATION_UTC_OFFSET_MINUTES=60
AGGREGATION_RUN_ON_STARTUP=0
```

Then run:

```bash
docker compose -f dev_docker-compose.yml up -d --build aggregation
docker compose -f dev_docker-compose.yml logs -f aggregation
```

This also starts the API and database if needed. The database image installs the
aggregation schema; existing databases are not migrated. Only one aggregation service may
run at a time (see [Run state](#run-state)).

With `AGGREGATION_ENABLED=0` (the default) the service stays idle without contacting the
API or the database. After changing environment settings recreate the service with
`up -d aggregation`; after editing the bind-mounted Python source use `restart aggregation`.

## Settings

| Variable | Default | Purpose |
|---|---|---|
| `AGGREGATION_ENABLED` | `0` | Enable the service (`1`/`0`) |
| `AGGREGATION_SCHEDULE` | `*/10 * * * *` | Five-field cron schedule, in UTC |
| `AGGREGATION_UTC_OFFSET_MINUTES` | `60` | Default bucket offset, from -840 to 840 minutes |
| `AGGREGATION_RUN_ON_STARTUP` | `0` | Run one cycle before waiting for the cron (`1`/`0`) |
| `AGGREGATION_WRITE_BATCH_SIZE` | `2000` | Result rows per COPY/upsert batch (1–10000) |
| `AGGREGATION_LOG_RESULTS` | `0` | Print every committed bucket as an `aggregation_result` JSON line (`1`/`0`) |
| `AGGREGATION_REQUEST_TIMEOUT_SECONDS` | `30` | Positive HTTP timeout |
| `AGGREGATION_API_URL` | Internal API root from Compose `SUBPATH` and `VERSION` | Complete API URL including the version path |
| `AUTHORIZATION` | Compose setting | Enable API login |
| `ISTSOS_ADMIN` / `ISTSOS_ADMIN_PASSWORD` | Compose settings | Administrator credentials for `/Login` and the database |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | `database` / `5432` / `istsos` | Database for run state and results |

Invalid settings stop the service at startup.

**Schedule.** The cron expression is interpreted in UTC by
[croniter](https://github.com/pallets-eco/croniter): `*/10 * * * *` runs at minutes 00,
10, 20, 30, 40, 50. Cycles are synchronous: a trigger that occurs while a cycle is still
running is skipped and logged, and the next cycle starts at the first trigger after the
end. There is no catch-up after downtime.

**Network.** Each cycle creates a new API client (and token when authorization is
enabled). Temporary network errors and HTTP 408/429/502/503/504 get up to three attempts,
one second apart. Pagination links generated with a public hostname are routed to the
configured API host, and links outside the API root are rejected. A failed Datastream is
logged and the others continue; failure to read the configuration list aborts the cycle.

**Shutdown.** SIGTERM/SIGINT stop waiting immediately, or finish the current request
before exiting; the run in progress is marked `interrupted`. The Compose stop grace period
is 100 seconds for the default 30-second HTTP timeout.

## One cycle and tests

Run a single cycle against the existing API. Stop the scheduled instance first, because
the database lock allows only one service:

```bash
docker compose -f dev_docker-compose.yml stop aggregation
docker compose -f dev_docker-compose.yml run --rm --no-deps \
  -e AGGREGATION_ENABLED=1 aggregation python -m aggregation --once
docker compose -f dev_docker-compose.yml up -d --no-deps aggregation
```

Add `--datastream-id N` to process only one configured Datastream. Exit codes: 0 success
(including an empty configuration list), 1 failed cycle or state error, 2 invalid settings.

Local tests:

```bash
python -m pip install -r aggregation/requirements.txt pytest
python -m pytest tests/aggregation
```

## Manual recomputation

Changing a configuration does not recompute existing results. To recompute a period,
stop the scheduled instance and run:

```bash
docker compose -f dev_docker-compose.yml stop aggregation
docker compose -f dev_docker-compose.yml run --rm --no-deps aggregation \
  python -m aggregation --once --datastream-id 13 \
  --recompute-from 2026-01-01T00:00:00Z --recompute-to 2026-02-01T00:00:00Z
docker compose -f dev_docker-compose.yml up -d --no-deps aggregation
```

All buckets intersecting the range are recalculated in full (then rolled up). Cursors and
the change watermark do not move during a manual recomputation.

## Calculation rules

**Modes.**
- `sampled`: requires a positive fixed `samplingFrequency` in the Datastream properties.
  Expected samples are counted from the bucket start (or the history start) up to the
  evaluated point; coverage = numeric count / expected count, not capped.
- `event`: each observation is an event; a `null` result (a tick) counts as 1. A bucket
  without events is valid and worth 0; a bucket whose events were all rejected is
  invalid. With a `referenceDatastreamId`, an empty bucket is 0 only if the reference has
  an observation within `referenceMaxAge` (default `PT1H`) before the bucket end
  (evaluated point for an open bucket); otherwise it is missing, so a silent station is
  not filled with zeros. Weeks, months and years apply the same rule at their end. A late
  reference observation recomputes the buckets it proves (changed times with versioning,
  the recompute window without). This is the old `zero_when_available` job rule.

**Values.** Only finite JSON numbers contribute (plus event ticks); booleans, strings, nulls and the
`missingValues` markers (default `[-999.9]`) do not. `conversionFactor` (default 1)
multiplies the values after the marker check.

**Input QC.** With `ignoreQuality=true` every observation counts. Otherwise human
acceptance/rejection wins, and the automatic level (3 all checks passed, 2 mixed, 1 all
failed, 0 not run) must be `>= minInputQuality`; missing QC is level 0, malformed QC is
rejected. `resultQualityKey` selects a key inside object-valued QC. If the Datastream QC
`constraints` change, the service stops that configuration until a manual recomputation.

**Validity.** A bucket is valid when it has values (or is an empty event bucket) and meets
`minCoverage`, `minObservations` and, for 10 minutes, hours and days only,
`maxConsecutiveMissing`. Invalid buckets keep their real statistics, but the API shows
`-999.9`. Buckets without values store `NULL` statistics (`-999.9` in the API), except
valid empty event buckets, which store 0.

**Output QC.** `result_quality` packs 2-bit pairs: data (`11` valid, `01` valid empty
event, `00` invalid), one pair per automatic check (`10` if a contributing value failed
it, `11` if all passed, else `00`), human (`00`, not reviewed) and aggregation (`11`
valid, `10` invalid).

**Alignment.** Buckets are right-closed `(start, end]` by default (`left_closed` is
supported) and aligned in a fixed UTC offset (`utcOffsetMinutes`, default 60), without
daylight saving. `startTime` is an inclusive lower bound; `endTime` caps the upper bound.
Only point observations are supported.

**Rollup.** Weeks, months and years are combined from the saved daily rows: counts, sums,
minima, maxima and expected counts add up exactly; mean, coverage and validity are
recomputed from the sums; check pairs are `10` if any day failed, `11` if all days with
values passed. Coverage matches the direct calculation when `samplingFrequency` divides
one day.

**Open buckets.** A bucket whose cutoff falls inside it is stored with
`evaluated_until < bucket_end` and recalculated on the next run.

## Versioning and late data

The service detects versioning from the database schema. With versioning it finds the
changed observations with one API request per Datastream on the current observations:
`$filter=lower(systemTimeValidity) ge <last scan> and lower(systemTimeValidity) le <cutoff>`.
Inserts and updates open a new version, so this returns exactly the changed observations
(index `datastream_id, lower(systemTimeValidity)`). The `phenomenonTime` of an observation
is assumed never to change; deleted observations are not detected, so after a deletion run
a manual recomputation of that period. The changed buckets are read with `$as_of=<cutoff>`. Without `startTime`, a backdated
insert moves the history start back.

The cutoff comes from `aggregation.safe_change_watermark()`, which stays behind the oldest
open transaction so late commits are not missed; scans overlap by one second. A session
left idle in a transaction freezes the watermark until it is closed. Prepared transactions
pause versioned runs.

Without versioning, only the bootstrap run and the look-back window are recalculated
automatically; older edits or deletions need a manual recomputation.

## Run state

`aggregation.processing_state` keeps the latest run of each configuration, readable at
`/Aggregations(id)/ProcessingState` (administrator only). The service records `running`,
then `success`, or `failed` on source, metadata or calculation errors; a failed read of the
configuration list marks all enabled configurations `failed`. Failures increment
`consecutiveFailures`; a success clears the error, resets the counter and updates
`lastSuccessfulAt`.

At startup the service takes a PostgreSQL session advisory lock: a second instance fails.
It then marks rows left `running` by a previous instance as `interrupted`. Every state
update is fenced by the run UUID. Losing the database connection stops the service, so
Docker restarts it and it recovers the lock; it never continues without reliable state.

Results and checkpoint of each batch commit in one transaction; a failed batch rolls back
both, and the next run resumes from the last committed checkpoint. A configuration edit,
disable or delete during a run makes the following batches fail instead of writing results
with stale settings.

## Logs

Diagnostic logs go to stderr: configuration reads, metadata warnings, per-Datastream
results and failures. `Window read` lines report buckets, pages, observations and elapsed
time; `Bulk write` lines report table, rows and elapsed time. With
`AGGREGATION_LOG_RESULTS=1` every committed bucket is also printed on stdout as an
`aggregation_result` JSON line.

## Code structure

| Module | Role |
|---|---|
| `service.py` | settings, API client, one cycle over all configurations, cron scheduler, entry point |
| `state.py` | database connection: ownership lock, run state, watermark, daily rows, result writes |
| `engine.py` | per configuration: cutoff, changes, buckets to calculate, source windows, checkpoints, rollup |
| `calculation.py` | bucket alignment, input QC filter, accumulator, validity, output bitmask, rollup of daily rows |

### `service.py`

- **`Settings.from_env()`** reads and validates the environment variables.
- **`SensorThings`** wraps `istsos4-client` (login, timeout, retries) and adds raw GET
  requests for the custom `/Aggregations` endpoint: `configurations()`, `datastream()`,
  `observation_pages()` (pages of 5000, repeated links rejected), `first_observation()`,
  `last_observation()` (reference Datastream), `changed_times()` (filter on
  `lower(systemTimeValidity)`) and `observations()` (one source window, with `$as_of`
  when versioned). `url()` keeps pagination on the configured host.
- **`metadata_warnings()`** lists problems in the Datastream metadata (missing
  `samplingFrequency` in sampled mode, invalid or too many `constraints`).
- **`run_cycle()`** runs one cycle as described above; errors mark that configuration
  `failed` (or `interrupted` while stopping) and the others continue.
- **`serve()`** waits for the next cron trigger and runs cycles synchronously.
- **`main()`** parses `--once`, `--recompute-from/--recompute-to` and `--datastream-id`,
  installs the signal handlers and runs `--once` or `serve()`.

### `state.py`

`ProcessingState` holds one autocommit connection with `statement_timeout=10s`.

- **`open()`** takes the advisory lock and marks leftover `running` rows `interrupted`.
- **`execute()`** turns any database error into `StateError`, which stops the service.
- **`start()`** / **`finish()`** open and close a run, fenced by `run_id`.
- **`discovery_failed()`** marks all enabled configurations `failed` when the
  configuration list cannot be read.
- **`checkpoint()`** reads the stored checkpoint; **`daily_rows()`** reads saved daily
  buckets for the rollup.
- **`source_context()`** detects versioning and returns the cutoff.
- **`save_buckets()`** writes one batch in one transaction: checks that all rows share
  `configuration_updated_at`, locks state and configuration rows (`FOR UPDATE`) and checks
  that the run still owns the state and the configuration is enabled and unchanged, COPYs
  into a session temporary table, upserts into the result table and saves the checkpoint.
  Connection errors become `StateError` (restart); other database errors become
  `CalculationError` (that configuration fails, the cycle continues).
- **`complete()`** saves the final checkpoint and marks the run `success`.

### `engine.py`

- **`calculate()`** processes one configuration: QC layout check, cutoff, history start,
  change scan, plans per source interval (changed or look-back buckets behind the cursor,
  then the bootstrap run), one read per source window for all three intervals, batch
  writes with per-interval cursors (a cursor advances only with its own committed rows;
  an open bucket keeps it at its start), rollup, final checkpoint.
- **`recompute_window()`** returns the look-back window without versioning.
- **`roll_up()`** reads the saved days of every touched week, month and year and writes
  them through `calculation.rollup()`.
- **`result_record()`** builds the `aggregation_result` log line.

### `calculation.py`

- **`bucket_floor()`**, **`next_bucket()`**, **`containing()`**: bucket alignment in the
  fixed UTC offset and closure.
- **`timestamp()`**: ISO datetime with timezone to UTC; point observations only.
- **`sampling_seconds()`**: `samplingFrequency` in seconds (sampled mode).
- **`quality()`**: input QC filter.
- **`Accumulator`**: one bucket; `add()` updates counts, sum, min, max, gaps and QC flags
  one observation at a time; `result()` computes expected count, coverage, validity and
  the output bitmask.
- **`rollup()`**: combines saved daily rows into one week, month or year row.
- **`is_valid()`**, **`output_mask()`**, **`result_row()`**: shared by `Accumulator` and
  `rollup()`.
