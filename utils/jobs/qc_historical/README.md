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

For QC only, a user with the `qc` role works with `COMMIT_MESSAGE` empty,
as in Flink (`API_USERNAME_QC`). To use an `administrator`, `editor`, or
`obs_manager` account, set `COMMIT_MESSAGE`: the API requires it for updates.

## Run

```bash
docker run --rm --network host --env-file .env \
  ghcr.io/istsos/istsos4/utils/qc_historical:0.1
```

One line per datastream: observations checked, `resultQuality` updated.

## Set a fixed resultQuality

Set `SET_FLAG` to an integer to write that `resultQuality` on every
observation in `PERIOD`, no-data ones included, instead of running QC. It
applies to the datastreams QC would check. As with QC, observations that
already have that value are not PATCHed. Empty runs QC.

## Fill missing observations

Set `FILL_MISSING=true` to create missing observations before checking quality.
The step comes from `properties.samplingFrequency`, a positive, fixed ISO 8601
duration such as `PT10M`. Each created observation has `result: 999.9` and
`resultQuality: "0"`. QC treats `999.9` as no data, including in existing
observations; the older negative no-data markers remain supported.

One account can fill gaps and update quality. Leave `ISTSOS4_FILL_USERNAME`
and `ISTSOS4_FILL_PASSWORD` empty to reuse the main account, and set
`COMMIT_MESSAGE` for both operations. For example, with an admin account:

```dotenv
ISTSOS4_USERNAME=admin
ISTSOS4_PASSWORD=your-password
COMMIT_MESSAGE=Fill historical gaps and recompute QC
FILL_MISSING=true
```

The account needs read, insert, and update permissions for the selected
datastreams. An `editor` or `obs_manager` account with those permissions can
also do both operations. The `qc` role cannot insert observations.

Separate insert credentials remain optional: set both `ISTSOS4_FILL_USERNAME`
and `ISTSOS4_FILL_PASSWORD` to keep a QC account for updates. With a separate
`sensor` account, leave `FILL_COMMIT_MESSAGE` empty; with a separate
`administrator`, `editor`, or `obs_manager`, set it. When using the main account
for filling, `FILL_COMMIT_MESSAGE` can override `COMMIT_MESSAGE` for inserts.

Only the selected `PERIOD` is filled, with inclusive bounds. An omitted start
or end uses the first or last existing observation in the period; supplying
both bounds also allows filling an empty period. The grid is anchored to the
last observation before the period, otherwise its first observation, otherwise
the explicit start. Boundaries are filled only when they lie on that grid.
For example, with `PT10M`, observations at `00:00` and `00:30` gain no-data
observations at `00:10` and `00:20`.

Existing observations are preserved, and repeating the run creates no more
rows. A missing or invalid frequency skips filling and still runs QC. Off-grid
timestamps cause that datastream to be reported and skipped before any writes.
Created observations participate in QC buffers, so filling gaps can change
the quality of subsequent observations. Only datastreams with constraints are
processed, using the same datastream filters as QC.

## Same as Flink

- Datastreams: those with `constraints` in their `properties`, filtered by
  `DATASTREAMS` / `EXCLUDE_DATASTREAMS`.
- Each observation whose `phenomenonTime` is in `PERIOD` (bounds included)
  is checked the way Flink checks a new one: at its `phenomenonTime`, in a
  buffer of N observations sorted by `phenomenonTime`, N = largest window /
  `samplingFrequency` + 3 (50 without them). The first ones in `PERIOD` get
  the N-1 observations before it, read to fill their window, not checked.
  Only its own `resultQuality` is written, so e.g. the start of a flat line
  stays "passed".
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

- Flink's buffer holds the last N observations to arrive, here the checked
  one and the N-1 before it in `phenomenonTime`: the same for data that
  arrived in order. Late data is checked here as if it had arrived in order,
  with its full window, and is in the window of the observations after it.
- Only observations whose `resultQuality` changes are PATCHed, so a re-run
  writes nothing new.
- Any SaQC function runs. Flink runs only those in its
  `ALLOWED_CONSTRAINTS` (`IstsosQC.py`): add new ones there too.
- A datastream that fails is reported and skipped; the run goes on.
