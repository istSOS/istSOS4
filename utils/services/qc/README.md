# qc

Keeps `resultQuality` up to date. Every `QC_INTERVAL_MINUTES`, it runs the
check from [qc_historical](../../jobs/qc_historical/README.md) on what is new
in each datastream with `constraints`. Use qc_historical for entire series:
data that was there before, imports, or a re-check after editing
constraints. This service is the updater: each run reprocesses a short window.

## Run

Set the `QC_*` variables in the istSOS `.env` (see `.env.example`), then:

```bash
docker compose up -d   # builds the qc image if it can't pull it
docker compose logs -f qc
```

Each run logs one line per datastream it checks: observations checked, and
how many `resultQuality` values were updated. A run with nothing new logs
nothing.

Credentials work as for qc_historical. With a `qc` role user, leave
`QC_COMMIT_MESSAGE` empty. An `administrator`, `editor` or `obs_manager`
account needs one, and so does `VERSIONING=1` with `AUTHORIZATION=0`. With
`AUTHORIZATION=0`, leave the user empty.

The service talks to the API on the compose network, at `api:5000`. The
API's `@iot.nextLink` start with `HOSTNAME + SUBPATH + VERSION`, which the
container may not be able to reach (`localhost`, for example). So qc sends
those requests to `api:5000` too (`ISTSOS4_PUBLIC_URL` in the compose file).

## What is new

New means a `resultTime` after the previous run, minus `QC_OVERLAP_SECONDS`.
This works the same with or without versioning. It relies on one rule:
**`resultTime` is the arrival time on the server, and it is set again when a
result is edited.**

- New data, late data and edited results all get a fresh `resultTime`, so
  they are caught.
- QC's own `resultQuality` PATCHes leave `resultTime` as it is, so they don't
  come back on the next run.
- The overlap covers two cases: rows stamped before a run but committed after
  it, and clock differences between the writers and the `qc` container, whose
  clock is the watermark.

Each run checks from the oldest new `phenomenonTime` to the end of the
series. As in qc_historical, the N-1 observations before it fill the buffers,
and only `resultQuality` values that change are PATCHed. Late data therefore
also re-checks the observations after it. Rows in the overlap are checked
twice; the second check writes nothing.

The first run of a datastream checks all of it. If qc_historical already ran
on it, that run writes nothing.

## State

`/data/state.json`, in the volume `v-istsos4-qc-data`, keeps one watermark
per datastream id. It is written after each datastream that succeeds. A
datastream that fails is logged, keeps its watermark, and is retried on the
next run. To check one datastream in full again, delete its entry, or the
whole file, and restart the service.

Edited constraints apply to new observations only. Run qc_historical for the
rest.

## Parameters

| `.env` | Default | |
|---|---|---|
| `QC_USERNAME`, `QC_PASSWORD` | empty | istSOS user, `qc` role |
| `QC_COMMIT_MESSAGE` | empty | see [Run](#run) |
| `QC_INTERVAL_MINUTES` | `10` | between the start of two runs |
| `QC_OVERLAP_SECONDS` | `60` | look-back on `resultTime` |
| `QC_DATASTREAMS` | empty: all | names, comma-separated |
| `QC_EXCLUDE_DATASTREAMS` | empty: none | names, comma-separated |

`HOSTNAME`, `SUBPATH` and `VERSION` come from the istSOS settings.

## Limits

- **Writers must follow the `resultTime` rule.** Rows that break it are never
  checked by this service; use qc_historical for them. Today:
  - mqtt2istsos stamps the arrival time of each message.
  - xlsx2istsos and istsos2_to_istsos4 stamp the arrival time of each bulk.
  - eyeonwater2istsos stamps the arrival time of each observation.
  - istsos4_to_istsos4 copies the source `resultTime`, which already follows
    the rule.
  - ftp2istsos doesn't follow it: it uses the payload's `resultTime`, or
    `phenomenonTime`. To fix.
  - Edits through the API or a GUI follow it only if that client sets
    `resultTime`.
- New databases have an index on `(datastream_id, "resultTime")`, so each
  run reads only the new observations. A database created before it needs
  it created once: see `database/README.md`.
- Don't let Flink QC and this service check the same datastreams: the work
  would be done twice. Split them with `QC_DATASTREAMS` /
  `QC_EXCLUDE_DATASTREAMS`.

## Build and test

The image is built from `utils/`, so it includes the current
`qc_historical.py`:

```bash
cd utils
docker build -t ghcr.io/istsos/istsos4/utils/qc:0.1 -f services/qc/Dockerfile .
cd ..
docker run --rm --entrypoint python \
  -v "$PWD/utils/services/qc/test_qc.py:/app/test_qc.py:ro" \
  ghcr.io/istsos/istsos4/utils/qc:0.1 test_qc.py
```
