# Aggregation result storage

`database/aggregation_results.sql` creates six ordinary PostgreSQL tables
shared by all configured source Datastreams. It does not create background jobs. The Python service calculates and upserts results on its cron schedule. With
versioning, historical inserts, updates and deletes trigger bucket recalculation
through system-time change detection; without it, old changes require manual
recomputation. See the worker guide in `aggregation/README.md`.

## Fixed intervals

| Bucket interval | Table in schema `aggregation` |
|---|---|
| `PT10M` | `observations_10min` |
| `PT1H` | `observations_1hour` |
| `P1D` | `observations_1day` |
| `P7D` | `observations_7days` |
| `P1M` | `observations_1month` |
| `P1Y` | `observations_1year` |

Intervals are predefined, not configuration overrides. A month and year are
calendar intervals, not fixed 30/365-day buckets.

## Common columns

Each table has primary key
`(datastream_id, bucket_start)`. Recalculation replaces the current bucket row
using `INSERT ... ON CONFLICT ... DO UPDATE`.

| Column | PostgreSQL type | Meaning |
|---|---|---|
| `datastream_id` | `BIGINT NOT NULL` | Source Datastream FK, `ON DELETE CASCADE` |
| `bucket_start` | `TIMESTAMPTZ NOT NULL` | Full bucket start |
| `bucket_end` | `TIMESTAMPTZ NOT NULL` | Full bucket end, strictly after start |
| `evaluated_until` | `TIMESTAMPTZ NOT NULL` | Processing cutoff within the bucket, inclusive |
| `count_total` | `BIGINT NOT NULL` | Input observations before QC filtering |
| `count_accepted` | `BIGINT NOT NULL` | Observations accepted by the QC rules |
| `count_numeric` | `BIGINT NOT NULL` | Accepted numeric values contributing to statistics |
| `sum_value` | `DOUBLE PRECISION` | Sum of contributing numeric values |
| `min_value` | `DOUBLE PRECISION` | Minimum contributing value |
| `max_value` | `DOUBLE PRECISION` | Maximum contributing value |
| `avg_value` | `DOUBLE PRECISION` | Mean calculated by the worker |
| `expected_count` | `BIGINT` | Expected sample count; null when not applicable, such as ticks |
| `coverage` | `DOUBLE PRECISION` | Numeric count / expected count; null when expectation is unknown or zero |
| `result_quality` | `BIGINT NOT NULL` | Packed output QC including aggregation status |
| `constraint_count` | `SMALLINT NOT NULL` | Number of automatic source QC checks used to interpret this result |
| `calculated_at` | `TIMESTAMPTZ NOT NULL` | Calculation timestamp, defaults to `clock_timestamp()` |
| `configuration_snapshot` | `JSONB NOT NULL` | Object containing effective calculation settings and source QC metadata |

Open buckets keep their full `bucket_end` and record their actual
`evaluated_until`. The worker determines alignment, right/left closure, timezone
offset, expected sample count at the cutoff, and interval-specific boundaries.
The database checks time ordering but does not calculate bucket boundaries.

Counts satisfy `0 <= count_numeric <= count_accepted <= count_total`. When
`count_numeric` is zero, all four numeric statistics must be SQL null; otherwise
all four must be finite. Minimum cannot exceed maximum. The worker calculates
sum/mean consistency and coverage; these are stored values, allowing the same
calculated frame to be written to SQL and Parquet. Coverage can exceed one for
oversampled data; it is an uncapped ratio, distinct from the configured
`minCoverage` threshold (0–1).

No-data markers such as `-999.9` must not be included in numeric statistics.
The worker prints the agreed missing-result and empty-tick
zero values. Empty buckets and buckets with rejected input remain distinguishable
through the counts and QC. Rollups must combine sums and numeric counts for a
weighted mean, rather than averaging averages or counting output markers.

`result_quality` is nonnegative. Its two-bit slots contain the data status,
then automatic checks, human review, then aggregation status. The human slot
therefore depends on `constraint_count`; the aggregation slot follows it.
The count is limited to 28 so all slots fit in a nonnegative signed `BIGINT`.
The table validates mask width, not the business meaning of each status. The
worker and client decoder must implement those meanings together.

There is no FK to the configuration table: removing a configuration retains
results. Deleting the source Datastream cascades to its results. Results keep
only the latest computed state, with no SensorThings system-time history,
compression or retention policy. `configuration_snapshot` is a required object;
the worker must populate effective settings rather than only overrides.

## Reading results: `GET /Datastreams(id)/Aggregates`

Results are exposed per Datastream, shaped like Observations. Two parameters
are required:

- `$func`: `sum`, `avg`, `min`, `max` or `count` (the value returned as `result`);
- `$interval`: `PT10M`, `PT1H`, `P1D`, `P7D`, `P1M` or `P1Y` (the result table).

```
GET /Datastreams(10)/Aggregates?$func=sum&$interval=P1D&$filter=phenomenonTime ge 2026-11-16T00:00:00Z
```

Each bucket is returned as:

| Field | Value |
|---|---|
| `phenomenonTime` | `bucket_start/bucket_end` |
| `result` | The `$func` statistic. A missing value is `-999.9`, or `0` for event series |
| `resultTime` | `calculated_at` |
| `resultQuality` | `result_quality` (validity of the bucket) |
| `parameters` | `countTotal`, `countNumeric`, `expectedCount`, `coverage`, `evaluatedUntil`, `open` (bucket still being filled) |

There is no `@iot.id`. Supported options: `$filter` on `phenomenonTime`
(compared with the bucket end), `resultTime`, `result` and `resultQuality`,
with `eq/ne/lt/le/gt/ge/in` joined by `and/or/not`; `$orderby` on the same
fields; `$top`, `$skip`, `$count`, `$select` and `@iot.nextLink`. `$expand`,
`$as_of` and `$from_to` are rejected: results are not versioned.

The query runs with the caller's role: a Datastream the caller cannot read
returns 404, and so does a Datastream without an aggregation configuration.
With authorization enabled, `aggregation_results.sql` grants read access on the
schema to the `user`, `guest` and `sensor` roles.

## Installation

New database images install the aggregation schema automatically: `database/Dockerfile`
loads `aggregation_configuration.sql`, `aggregation_results.sql`,
`aggregation_processing_state.sql` and `aggregation_engine.sql`, in this order.
Existing databases are not migrated: rebuild them from the image.
The scripts require the istSOS schema and `administrator` role, and work with
versioning enabled or disabled.
