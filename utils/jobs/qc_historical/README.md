# qc_historical

Recomputes the `resultQuality` of observations already in istSOS4 exactly as
the Flink QC job does, reading them through
[istsos4-client](https://github.com/istSOS/istSOS4-client) instead of Kafka.
Use it for data that never went through Flink (xlsx2istsos, ftp2istsos,
migrations), or to re-check a period after editing constraints.

Constraints, slots and `00` rules are Flink's: see `QC_CONSTRAINTS.md` in the
flink repository.

## Setup

```bash
cp .env.example .env    # istSOS4 URL and credentials, PERIOD, datastreams
docker build -t ghcr.io/istsos/istsos4/utils/qc_historical:0.1 .
```

Write `.env` values without quotes: `docker run --env-file` keeps them.

## Run

```bash
docker run --rm --network host --env-file .env \
  ghcr.io/istsos/istsos4/utils/qc_historical:0.1
```

One line per datastream: observations checked, `resultQuality` updated.

## Same as Flink

- Datastreams: those with `constraints` in their `properties`, filtered by
  `DATASTREAMS` / `EXCLUDE_DATASTREAMS`.
- Each observation in `PERIOD` is checked the way Flink checks a new one:
  with the N-1 observations before it, N = largest window /
  `samplingFrequency` + 3 (50 without them). Only its own `resultQuality` is
  written, so e.g. the start of a flat line stays "passed".
- Each constraint is the same SaQC call: `type` is the SaQC function, the
  other keys its arguments. One that SaQC rejects leaves its slot `00`.

## Speed

Flink makes one SaQC call per constraint and observation (~10 ms). Here the
buffers of thousands of observations are laid end to end, far apart in time,
and checked with one call: a year of 10-minute data takes seconds.

This holds for checks that look at a time `window` ("6h") or at each value.
A check that looks at all the data (`flagZScore` without `window`) or counts
values (`"window": 6`) would see other buffers: a sample of buffers checked
on their own catches this, and that constraint then runs one buffer at a
time, as slow as Flink. Prefer time windows.

## Different from Flink

- Observations are taken in `phenomenonTime` order, Flink takes them in
  arrival order: the same for data that arrived in order.
- Only observations whose `resultQuality` changes are PATCHed (commit message
  `QC update`), so a re-run writes nothing new.
- Any SaQC function runs. Flink runs only those in its
  `ALLOWED_CONSTRAINTS` (`IstsosQC.py`): add new ones there too.
- A datastream that fails is reported and skipped; the run goes on.
