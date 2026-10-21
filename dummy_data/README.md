# Dummy data

This container generates a random dataset to mimic a real-world monitoring network. It allows you to create a specified number of entities ("things") for each observed property, with observations over a defined time interval at a given frequency. Additionally, it includes information about locations, historical locations, features of interest, sensors, datastreams, and observations associated with the things.

## Input Parameters

- `N_THINGS (int)`: Number of things being observed in the dataset.
- `N_OBSERVED_PROPERTIES` (int): Number of different properties or features being recorded for each thing.
- `INTERVAL` (str): Time interval over which the data is generated, following the ISO 8601 duration format (e.g., "P1Y" for a period of 1 year).
- `FREQUENCY` (str): Frequency at which data points are recorded, using the ISO 8601 duration format (e.g., "PT30M" for a period of 30 minutes).
- `START_DATETIME` (str): Specifies the start date for phenomenonTime

## Entities Counts

- `Locations`, `HistoricalLocations`, and `FeaturesOfInterest`: One per thing.
- `Sensors` and `Datastreams`: : One per thing per observed property.
- `Observations`: Number of observations for each datastream depends on the frequency and interval (e.g., one observation every 30 minutes for one year).

## Data Generation Options

- `DUMMY_DATA` (bool): Specifies whether the dataset should be populated with dummy data.
- `CLEAR_DATA` (bool): Specifies whether the dataset should be cleared before generating new data.

By adjusting these parameters, you can create a customized dataset that suits your needs for testing and development purposes.

## Bulk loading and progress

Observations use binary PostgreSQL `COPY`. `BATCH_SIZE` (default `10000`)
caps the number of rows in memory. A batch is flushed when this cap is reached,
when `CHUNK_INTERVAL` is reached, or at the end of a Datastream. Each batch
contains only one Datastream. Larger time chunks are supported without the old
multi-row INSERT parameter limit.

Metadata is committed in one transaction. Each observation batch then commits
its observations, phenomenonTime/resultTime ranges, last FeatureOfInterest and
observedArea together. Areas use the distinct features tracked while generating
the stream, without rereading observations. Indexes, foreign keys, authorization
and versioning triggers stay enabled. Initial loading also supports entity
sequences that do not start at 1.

Every `LOG_INTERVAL_SECONDS` (default `5`), after a successful batch commit, the
loader reports committed rows, percentage and observations/second. At completion
it reports total time and a machine-readable `STATS` JSON record with generation,
COPY, derived-field, transaction, metadata and ANALYZE timings. Progress is checked
at batch boundaries, so slow database operations can delay the next log.
`RANDOM_SEED` optionally makes generated random values repeatable.

Example loading settings:

```dotenv
BATCH_SIZE=10000
CHUNK_INTERVAL=P1Y
LOG_INTERVAL_SECONDS=5
RANDOM_SEED=0
```

Use an initialized database and run one loader at a time. Another invocation of
this loader is rejected by an advisory lock. Stop other writers for a bulk-loading
window; they do not participate in this lock. Observation and time sampling rules
remain the same: the first sample follows `START_DATETIME` by one `FREQUENCY`, and
the final sample may exceed the interval if it is not evenly divisible.

On failure the process exits nonzero and only the current transaction rolls back;
earlier batches remain committed. Set `RESUME=1` to continue an interrupted load
as described below. Without this option, duplicate generated Thing names are
rejected. **`CLEAR_DATA=1` deletes all core data, including non-dummy data**;
it preserves the configured authorization User and does not reset sequences.
Clearing is transactional and maintains normal versioning behavior.

## Resume an interrupted load

`RESUME` defaults to `0`. Set `RESUME=1`, `CLEAR_DATA=0` and `DUMMY_DATA=1` to
reuse the existing metadata, count stored observations per Datastream and append
only the missing tail of each stream. This also supports data written by the
original loader, including stale Datastream time ranges and observed areas.
Completed streams are skipped, and rerunning resume after completion adds no rows
or Commits. Entity IDs do not need to start at 1.

**Stop the previous loader and other writers before resuming.** Keep the same
`N_THINGS`, `N_OBSERVED_PROPERTIES`, `START_DATETIME`, `FREQUENCY`, `INTERVAL`,
`VERSIONING`, `AUTHORIZATION` and `NETWORK` settings used for the original load.
With the development Compose file:

```bash
docker compose -f dev_docker-compose.yml build dummy_data
docker compose -f dev_docker-compose.yml stop dummy_data
docker compose -f dev_docker-compose.yml run --rm --no-deps \
  -e RESUME=1 -e CLEAR_DATA=0 -e DUMMY_DATA=1 dummy_data
```

For the production Compose file, first rebuild/publish and select the updated
loader image, then use that file for the stop/run commands.

Resume checks all expected entity names and relationships, then reads existing
observations to verify each stream is a continuous prefix of the configured
sampling schedule. This initial check can take time on very large datasets; logs
show checked streams, the existing count/percentage and the remaining count.
It refuses incomplete metadata, duplicate timestamps, holes or incompatible
sampling settings before adding observations. Filling holes in existing data is
not supported. `RESUME=1` combined with `CLEAR_DATA=1` is rejected before deleting
anything. A conflicting active writer causes the initial table-lock attempt to
fail immediately; normal reads remain allowed.

After validation, cached time ranges, last FeatureOfInterest and observed areas
are repaired from the actual stored data. Existing observations stay unchanged.
Each new batch commits its rows and derived fields together. Versioned/authorized
loads create a new CREATE Commit for appended rows and keep existing Commit
references. Random values in the appended tail may differ from an uninterrupted
run, even with the same `RANDOM_SEED`; prior random draws are not replayed.

The final `STATS` record includes `existing_rows`, newly added `rows`,
`total_rows`, `planned_rows`, `streams_skipped` and `resume_check_seconds`.

The production Compose file uses a registry image: rebuild and publish that image
before expecting the new loader behavior there. For a local development build:

```bash
docker compose -f dev_docker-compose.yml build dummy_data
docker compose -f dev_docker-compose.yml run --rm --no-deps dummy_data
```

See [BENCHMARK.md](BENCHMARK.md) for measured before/after results and isolated
benchmark/integration-test commands.
