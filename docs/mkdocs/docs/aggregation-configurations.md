# Aggregation configurations

This administrator extension stores settings for the Python aggregation service.
The API stores settings; the separately scheduled Python service retrieves
observations, calculates statistics with pandas and writes results. Parquet
export and `/Datastreams(id)/Aggregate` are not implemented yet. The database initialization also
creates the six predefined [result hypertables](aggregation-results.md).

Paths below are relative to the SensorThings API root, normally
`/istsos4/v1.1`. They are custom configuration resources, not standard STA
entities; the full SensorThings/OData query language does not apply to them.

## Database installation

New database images install the table automatically. For an existing database,
apply `database/aggregation_configuration.sql` using the database owner/superuser:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/aggregation_configuration.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/aggregation_results.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/aggregation_processing_state.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/aggregation_engine.sql
```

All scripts are idempotent. The configuration script removes the obsolete
`intervals` column from initial installations, preserving the other settings.
Intervals are fixed for all configured Datastreams: `PT10M`, `PT1H`, `P1D`, `P7D`,
`P1M`, `P1Y`; POST/PATCH reject an `intervals` field. Installation requires the existing istSOS schema and
`administrator` role, and works independently of versioning and duplicate-name
settings. Missing storage returns an actionable `503` response; API requests do
not perform schema changes.

## Endpoints

| Method | Path | Result |
|---|---|---|
| POST | `/AggregationConfigurations` | Create; `201`, body and `Location` header |
| GET | `/AggregationConfigurations` | List; `200`, `value` array |
| GET | `/AggregationConfigurations(id)` | Read one; `200` |
| PATCH | `/AggregationConfigurations(id)` | Update editable settings; `200`, complete updated body |
| DELETE | `/AggregationConfigurations(id)` | Delete configuration; `204`, no body |
| GET | `/AggregationConfigurations(id)/ProcessingState` | Read latest service-managed run state |

When authorization is enabled, all operations require the `administrator` role.
When authorization is disabled, they follow the server's unauthenticated mode.
POST and PATCH require JSON bodies with `Content-Type: application/json`.

There is exactly one configuration per source Datastream. A duplicate POST
returns `409`; an invalid/nonexistent source reference returns `400`. Missing
configuration IDs return `404` for GET, PATCH and DELETE. Invalid body values
return `400`; invalid typed path/query parameters return FastAPI's `422`.

List requests support `datastreamId`, `enabled`, `$top` (1–1000, default 100),
and `$skip` (default 0). Results are ordered by configuration ID. Follow
`@iot.nextLink` until absent; it preserves the filters and page size.

## Fields

Only `datastreamId` and `minInputQuality` are required on creation. Optional
processing overrides default to `null`, meaning the worker should use
its configured defaults. This API stores overrides, not resolved worker values.

| Field | Type / validation | Editable with PATCH |
|---|---|---|
| `id` | Server-generated positive integer | No |
| `datastreamId` | Required positive integer, existing source Datastream | No |
| `minInputQuality` | **Required** integer `0`–`3`; cannot be null | Yes |
| `ignoreQuality` | Boolean; default `false`, cannot be null | Yes |
| `enabled` | Boolean; default `true`, cannot be null | Yes |
| `mode` | `sampled` or `event`; default `sampled`, cannot be null | Yes |
| `minCoverage` | Null, or finite fraction from `0` through `1` | Yes |
| `minObservations` | Null, or nonnegative 32-bit integer | Yes |
| `maxConsecutiveMissing` | Null, or nonnegative 32-bit integer | Yes |
| `conversionFactor` | Null, or finite number | Yes |
| `boundaryMode` | Null, `right_closed` or `left_closed` | Yes |
| `utcOffsetMinutes` | Null, or integer from `-840` through `840` | Yes |
| `bucketsToRecompute` | Null, or positive 32-bit integer | Yes |
| `startTime` / `endTime` | Null, or ISO 8601 datetime with explicit timezone; stored in UTC | Yes |
| `resultQualityKey` | Null, or nonblank JSON quality-object key, at most 255 characters | Yes |
| `missingValues` | Null, or array of finite numeric input missing markers; `[]` explicitly selects none | Yes |
| `createdAt` / `updatedAt` | Server-managed timestamps | No |
| `@iot.selfLink` | Server-generated configuration URL | No |
| `Datastream@iot.navigationLink` | Server-generated source Datastream URL | No |

When both processing dates are set, `startTime` must precede `endTime`; either
bound can be unset for an open-ended processing period. These dates describe
the processing domain, not the time of a manual recomputation request.

`ignoreQuality: true` disables all quality filtering: missing QC, automatic
failures and human rejection do not exclude observations. Numeric validity and
configured missing-value checks still apply. The default `false` preserves QC
filtering. `minInputQuality` remains required and stored while quality is ignored,
so disabling the bypass restores the chosen threshold. For example:

```http
PATCH /AggregationConfigurations(1)
Content-Type: application/json

{"ignoreQuality": true}
```

The service prints this policy as `qualityFiltering: "disabled"` in metadata
logs and applies it during pandas calculations.
Changing this setting does not automatically recompute historical results.

When quality filtering is enabled, the threshold applies to the decoded **automatic** classification, not to
the raw bitmask: `0` raw/unexecuted, `1` all failed, `2` mixed/partial, `3` all
passed. The worker must first honor human acceptance/rejection; only
unreviewed observations reach this threshold. The user must choose the level
explicitly, including `0` if that is intended. Unmapped/invalid source QC still
needs explicit handling by the worker; the configuration API does not
decode observations or change the client decoder.

`samplingFrequency` and QC `constraints` remain on the source Datastream;
they are not copied into this table. Nor is an `expectedObservationInterval`
required in configuration: the worker reads it from source metadata.
Adding/reordering constraints requires manual QC remapping/reprocessing before
aggregate recalculation because the current human slot depends on their count.

For `event` mode, `minCoverage`, `minObservations` and `maxConsecutiveMissing`
must all be null. A minimum expected tick count is not meaningful for sparse
events. The event rule is zero for genuinely empty buckets; received
but QC-rejected events remain distinct. Sample-completeness defaults are not
applied in event mode. Prepared tick values/conversions can be supplied by a
virtual process; `conversionFactor` remains an optional explicit override.

## Examples

Create a sampled configuration:

```http
POST /AggregationConfigurations
Content-Type: application/json

{
  "datastreamId": 13,
  "minInputQuality": 3,
  "minCoverage": 0.8,
  "maxConsecutiveMissing": 2
}
```

Update several settings, preserving all omitted fields:

```http
PATCH /AggregationConfigurations(1)
Content-Type: application/json

{
  "enabled": false,
  "minInputQuality": 2,
  "minCoverage": null
}
```

`minCoverage: null` resets the override; it does not store a zero threshold.
Setting `minInputQuality` to null is rejected. Empty patches, unknown fields,
read-only fields and old job-only fields such as `targetDatastream` or
`scheduleInterval` are rejected. PATCH locks the current row and validates the
complete merged record, so simultaneous updates do not lose unrelated edits.

Switch to event mode and clear existing sampled completeness overrides together:

```json
{
  "mode": "event",
  "minCoverage": null,
  "minObservations": null,
  "maxConsecutiveMissing": null
}
```

## Deletion, recomputation and legacy jobs

DELETE removes only the configuration. The source Datastream, observations and
any existing calculated results remain. Disabling with PATCH is reversible;
DELETE removes the settings. Conversely, deleting the source Datastream removes
its configuration and stored aggregate results through their source foreign
keys' `ON DELETE CASCADE`.

Changing calculation settings does not automatically request historical
recomputation. Use a separate manual recomputation when historical results must change. This release has no
recomputation endpoint; the service supports manual recomputation through its CLI.

The previous `/AggregationJobs` routes and their OpenAPI entries are removed.
The legacy SQL procedures and already registered TimescaleDB jobs are left
untouched. Operators manage production job cleanup separately; this migration
does not disable/delete jobs or delete previous aggregation Datastreams.

## Tests

Install the API dependencies and the existing HTTP/pytest test dependencies in
your development virtual environment:

```bash
python -m pip install -r api/requirements.txt -r aggregation/requirements.txt -r tests/conformance/requirements.txt
```

Validation and route-registration tests:

```bash
DEBUG=0 AUTHORIZATION=0 VERSIONING=0 NETWORK=0 \
  python -m pytest tests/unit
```

Integration tests create/drop uniquely named databases and temporary login roles
on a **disposable** PostgreSQL/TimescaleDB server. They load the real istSOS
schema and run HTTP CRUD in both versioned and unversioned modes, including
duplicate-name support, FK cascade, pagination, merged PATCH validation and
administrator enforcement. They also verify the six result hypertables,
upserts, statistics constraints, result retention, source deletion, and
idempotent installation. The login needs database/role creation privileges.

```bash
DEBUG=0 AUTHORIZATION=0 VERSIONING=0 NETWORK=0 \
  AGGREGATION_TEST_DSN="$TEST_DATABASE_URL" \
  python -m pytest tests/integration/test_aggregation_configuration.py
```

## Processing state

`aggregation.processing_state` is a normal PostgreSQL table with one row per
configuration, linked by a primary-key foreign key with `ON DELETE CASCADE`.
Existing configurations are backfilled as `never_run`; new configurations get
a state row through a database trigger. Reapplying installation preserves states.
Deleting configuration removes its state but keeps aggregate results.

Each configuration response contains `ProcessingState@iot.navigationLink`.
The GUI can fetch it using the same administrator credentials:

```http
GET /istsos4/v1.1/AggregationConfigurations(1)/ProcessingState
```

| Response field | Meaning |
|---|---|
| `configurationId` | Configuration foreign key |
| `status` | `never_run`, `running`, `success`, `partial`, `failed`, `interrupted` |
| `phase` | `aggregation` for pandas calculations; `discovery` for earlier metadata-only runs |
| `runId` | Latest attempt UUID, null before first run |
| `lastStartedAt` / `lastFinishedAt` | Latest attempt timestamps; finish is null while running |
| `lastSuccessfulAt` | Last fully successful run, retained across subsequent failures |
| `lastError` | Null or an object with `step` and a safe `message`; metadata warnings are reported here for partial runs |
| `consecutiveFailures` | Consecutive failed, partial or interrupted attempts; reset by success |
| `checkpoint` | Nullable object containing durable bootstrap cursors, QC layout signature, domain start and completed change watermark |
| `updatedAt` | Last state change |

State is written by the service via SQL. Configuration POST/PATCH reject state
properties, and there is no API operation to modify the state. A nonexistent
configuration returns 404; missing state storage returns 503 with migration
instructions. The latest state is not an audit history.

A `success` in phase `discovery` means source metadata was retrieved without
warnings, not that statistics were calculated. Malformed/missing metadata gives
`partial`; API errors give `failed`. Missing required metadata therefore remains
visible even when a source request itself succeeds. For calculation runs, phase is `aggregation`: success means bucket writes
completed, while missing required metadata or API/calculation errors give
`failed`. Discovery remains the label for earlier metadata-only runs.

One service owns a PostgreSQL session advisory lock. After a crash, the next
owner marks remaining `running` states `interrupted` without changing their
checkpoints or last successful time. State updates are fenced by run UUID.
There is no heartbeat timeout: while the service is down, a row can still say
`running` until recovery. The GUI should display timestamps along with status.

Bulk bucket writes use COPY to a temporary staging table followed by one
upsert per batch. Each batch and its bootstrap checkpoint share a database transaction. The
system-time watermark advances only after all affected intervals finish. The
service supports manual recomputation via CLI; see the worker guide.
