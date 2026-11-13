# Aggregation service

The service reads enabled configurations and source observations through
`istsos4-client==0.4.0`, calculates statistics with pandas, and upserts the six
predefined result hypertables. It records per-Datastream status in PostgreSQL.
Source observations are not modified. Parquet export and concurrent Datastream
processing are not implemented.

Metadata records and individual `aggregation_result` records go to stdout;
diagnostic logs go to stderr. Invalid Datastreams fail independently and remain
visible through the processing-state API.

## Start in development

Set these in the repository `.env`:

```dotenv
AGGREGATION_ENABLED=true
AGGREGATION_SCHEDULE="*/10 * * * *"
AGGREGATION_UTC_OFFSET_MINUTES=60
AGGREGATION_RUN_ON_STARTUP=false
```

Then run:

```bash
docker compose -f dev_docker-compose.yml up -d --build aggregation
docker compose -f dev_docker-compose.yml logs -f aggregation
```

This starts the normal API/database dependencies if needed. For an existing
database, first install all aggregation schema scripts described in
[the configuration contract](../docs/mkdocs/docs/aggregation-configurations.md).
The Compose network must exist, as required by the existing development setup.
Only one aggregation service instance should run: overlap prevention is local
to the process. The production Compose file is unchanged.

The service is disabled by default. With `AGGREGATION_ENABLED=false` it remains
idle without contacting the API. After changing environment settings, recreate
the service with `up -d aggregation`; after editing the bind-mounted Python
source, use `restart aggregation`.

## Timing and settings

The five-field cron expression is interpreted in **UTC**. `*/10 * * * *`
executes at minutes 00, 10, 20, 30, 40 and 50. There is no immediate run unless
`AGGREGATION_RUN_ON_STARTUP=true`. The parser is
[croniter](https://github.com/pallets-eco/croniter), with normal cron Sunday
numbering (0 or 7) and day-of-month/day-of-week OR semantics. Seconds/year fields
and cron aliases are not accepted. Invalid settings fail at startup.

| Variable | Default | Purpose |
|---|---|---|
| `AGGREGATION_ENABLED` | `false` | Enable reads (`true`/`false` or `1`/`0`) |
| `AGGREGATION_SCHEDULE` | `*/10 * * * *` | Five-field cron schedule in UTC |
| `AGGREGATION_UTC_OFFSET_MINUTES` | `60` | Default bucket offset, from -840 to 840 minutes |
| `AGGREGATION_RUN_ON_STARTUP` | `false` | Perform one aggregation cycle before waiting for cron |
| `AGGREGATION_WRITE_BATCH_SIZE` | `1000` | Calculated result rows per COPY/upsert batch (1–10000) |
| `AGGREGATION_REQUEST_TIMEOUT_SECONDS` | `30` | Positive HTTP timeout |
| `AGGREGATION_API_URL` | Internal API root from Compose `SUBPATH` and `VERSION` | Complete URL including the version path |
| `AUTHORIZATION` | Compose setting | Enable API login |
| `ISTSOS_ADMIN` / `ISTSOS_ADMIN_PASSWORD` | Compose settings | Administrator credentials for `/Login` |

A Datastream's non-null `utcOffsetMinutes` overrides the service default,
including zero. This offset is reported for later calculations and does not
change cron timing. No daylight-saving timezone is used.

Runs are sequential. Triggers that occur while a run is still executing are
skipped and logged; there is no catch-up queue after downtime. Each cycle reads
configurations afresh and, when authorization is enabled, obtains a fresh API
token through the client. Temporary network errors and HTTP
408/429/502/503/504 receive up to three attempts, one second apart. HTTP 500
and authentication failures are logged and retried on the next cycle. A failed
Datastream is logged and the remaining Datastreams continue. Failure to read the
configuration collection aborts that cycle; the next scheduled cycle retries.
Pagination links generated with a public hostname are routed to the configured
API host. SIGTERM/SIGINT stop waiting immediately, or finish the current client
request (including its bounded retries) before shutdown. The Compose shutdown
grace period is 100 seconds for the default 30-second HTTP timeout; increase it
if you configure a longer timeout.

## One cycle and tests

Run a single cycle against the existing API. Stop the scheduled instance first
because a PostgreSQL session advisory lock permits only one state-writing service:

```bash
docker compose -f dev_docker-compose.yml stop aggregation
docker compose -f dev_docker-compose.yml run --rm --no-deps \
  -e AGGREGATION_ENABLED=true aggregation python -m aggregation --once
docker compose -f dev_docker-compose.yml up -d --no-deps aggregation
```

Exit codes are 0 for success (including an empty configuration list), 1 for a
failed cycle, and 2 for invalid settings. Disabled `--once` exits without reads.

For local tests:

```bash
python -m pip install -r aggregation/requirements.txt pytest
python -m pytest tests/aggregation
```

## Latest run status

Apply `database/aggregation_processing_state.sql` and
`database/aggregation_engine.sql` after configuration and result storage
on existing databases. New images install it automatically. The service uses
`POSTGRES_HOST`, `POSTGRES_PORT` (default 5432), `POSTGRES_DB`, `ISTSOS_ADMIN` and
`ISTSOS_ADMIN_PASSWORD` to write state directly to the same database. This is
independent of whether the source API requires authentication.

The GUI can follow `ProcessingState@iot.navigationLink` on a configuration, or
request `/AggregationConfigurations(id)/ProcessingState`. State is read-only
through the API and follows the configuration API's administrator permissions.
See [the state contract](../docs/mkdocs/docs/aggregation-configurations.md#processing-state).

The worker records `running`, then `success` when calculation completes,
or `failed` on source, metadata or calculation errors. A failed collection read records a discovery failure
for all currently enabled configurations. `phase: aggregation` identifies calculation runs. Checkpoints track durable
bootstrap cursors and the last completed system-time change scan.
Non-successful attempts increment the failure counter; a fully successful
attempt clears the error, resets the counter and updates `lastSuccessfulAt`.

After acquiring exclusive ownership at startup, the service marks abandoned
`running` rows `interrupted`. Another active instance cannot do this because it
cannot acquire the lock. Stale run IDs cannot finish a newer run. Losing the
state database connection stops the service so Docker can restart it and recover;
it does not continue processing without reliable state. This table stores only
the latest attempt, not history. No worker heartbeat or live stale-state expiry
is implemented: an interrupted status becomes visible when the service restarts.

## Calculation rules and recovery

All six intervals are calculated directly from source observations. Statistics
are numeric count, sum, minimum, maximum and mean. Only finite JSON numbers
contribute; booleans, numeric strings, nulls and configured missing markers do
not. A null `missingValues` uses `[-999.9]`; an explicit empty array uses none.
A null conversion factor is 1. Missing results are never summed as sentinels.

The default boundary mode is right-closed `(start, end]`. Left-closed
`[start, end)` is supported. Alignment uses the fixed UTC offset, default +60
minutes. Seven-day buckets start Monday; months and years follow the calendar.
Current buckets are calculated up to `evaluated_until`, keeping their full end.
`startTime` is an inclusive lower processing bound; `endTime` caps the upper
bound using the selected closure. The first bucket may therefore be partial.
Point observations are supported; nonzero-duration phenomenonTime intervals
fail explicitly until an allocation rule is defined.

On first run, history starts at `startTime` or the earliest source observation.
Set `startTime` to generate empty event buckets before the first received tick;
without either a configured start or any observations, there is no inferred
historical domain.
Each interval has a durable bootstrap cursor. Adjacent buckets are read together in windows of at most seven days and pages
of 2000 observations. Each page is partitioned into its target buckets and
pandas reduces those groups into bounded accumulators. Empty buckets are
created without making separate API requests. Larger calendar buckets are
read in successive seven-day windows. Calculated result rows are buffered per interval, up to
`AGGREGATION_WRITE_BATCH_SIZE` rows. PostgreSQL `COPY FROM STDIN` transfers the
whole batch to a reusable session-local temporary staging table; one
`INSERT ... SELECT ... ON CONFLICT` merges all rows into the target hypertable.
There are no individual target-row INSERT calls. The final smaller batch of
each interval is flushed too.

The merge and its checkpoint commit in the same transaction. Any COPY/merge
failure rolls back the entire batch. If observation retrieval fails before a
pending batch is written, that pending batch is discarded and its cursor is
not saved. Previously committed batches survive, and the next run resumes from
the last committed checkpoint. Result log records are printed after commit.
The PostgreSQL login needs temporary-table privileges (normally granted by
default). Staging is emptied automatically on commit and disappears on disconnect.

Every scheduled run also recalculates the current plus two previous buckets
per interval, or `bucketsToRecompute` buckets if set. Buckets are upserted by
source Datastream and bucket start, so repeated processing replaces results.
A configuration change/disable/delete during processing prevents subsequent
writes from the stale configuration. Changing settings does not automatically
recompute older results; use manual recomputation for them.

When `ignoreQuality=true`, all QC is bypassed, including human rejection and
malformed/missing quality. Numeric and missing-marker checks still apply.
Otherwise missing QC is level 0, malformed/reserved QC is rejected, human
acceptance/rejection takes priority, and unreviewed automatic level must be
`>= minInputQuality`. `resultQualityKey` selects a key inside object-valued QC;
null expects scalar QC. Human position follows the number of constraints.
Changing the source QC layout requires source QC remapping and explicit manual
recomputation before ordinary processing continues.

Sampled mode requires positive, fixed `properties.samplingFrequency`. Expected
sample slots are anchored at bucket start and clipped to processing bounds and
the current cutoff. Coverage is numeric count / expected count, uncapped;
`minCoverage`, `minObservations` and `maxConsecutiveMissing` are disabled when
null. Missing runs are estimated from accepted numeric timestamp gaps at that
frequency; duplicate/oversampled observations count as received values, not
additional expected slots. Conversion is applied after input missing markers.

For event mode, genuinely empty buckets produce zero in printed results.
Received but QC-rejected/invalid events produce missing output instead. Empty
statistics remain SQL null with zero numeric count, preserving weighted-rollup
semantics. Failed completeness also retains real SQL statistics but marks the
output QC as no data; printed values become -999.9. Original human review is
not copied as a review of the new aggregate (human pair 00). Automatic check
pairs are conservatively combined from contributing masks. The aggregation
pair after human review is 11 passed, 10 failed, 00 not computed, 01 reserved;
empty event zero uses the digital data pair 01. These definitions are stored in
the configuration snapshot; the separate client/GUI decoder still needs support
for the additional aggregation pair before displaying these masks as legends.

## Historical changes with versioning

The worker detects versioning from the source database schema. With versioning,
`$from_to` reads historical validity intervals through the API, including old
versions of deleted or moved observations. Both old and new phenomenon times
identify buckets to recompute from source `$as_of` reads. A backdated insert can
extend an implicit history start. The phenomenon-time `endTime` limit remains
separate from system-time visibility, so late inserts into a closed historical
domain are still processed.

`aggregation.safe_change_watermark()` keeps the system-time cutoff strictly
behind active database transactions; scans additionally overlap by one second.
The function owner needs superuser or `pg_read_all_stats` privileges. Prepared
transactions pause versioned runs until resolved. This avoids assuming that a
fixed overlap alone protects against arbitrarily late commits. The source API
must use the same database/versioning mode as the configured SQL connection.

The change watermark advances only after all affected intervals are durable.
A crash repeats the change scan and safely upserts any already written buckets.
Without versioning, only bootstrap and recent buckets update automatically;
older edits/deletes need manual recomputation. Historical rows must be retained
until processing has consumed them.

This initial implementation favors correctness over query minimization. Large
recent yearly/monthly buckets are reread from source, and `$from_to` can return
long-lived versions whose validity overlaps the scan even when unchanged. Page
reduction bounds numeric-frame memory, but large sources may require optimizing
change queries and incremental sufficient statistics after measuring load.
First-run backfill may span several cron triggers; overlapping triggers are
skipped while that run completes.

## Manual recomputation

Stop the scheduled instance to release its ownership lock, then recompute all
buckets intersecting a range for a configured Datastream. This recalculates full
buckets within the configuration's processing domain, not statistics truncated
to the manual range. Bootstrap cursors and the automatic change watermark do
not advance during manual recomputation.

```bash
docker compose -f dev_docker-compose.yml stop aggregation
docker compose -f dev_docker-compose.yml run --rm --no-deps aggregation \
  python -m aggregation --once --datastream-id 13 \
  --recompute-from 2026-01-01T00:00:00Z --recompute-to 2026-02-01T00:00:00Z
docker compose -f dev_docker-compose.yml up -d --no-deps aggregation
```

Window-read logs report interval, buckets covered, pages, observation count and
elapsed time. Bulk-write logs report row count and elapsed time separately.
Adjacent bucket reads are grouped (up to seven days and the configured write
batch size), so empty periods do not produce one API request per missing bucket.
