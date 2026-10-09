# xlsx2istsos

Creates Locations, Things, Sensors, ObservedProperties, Networks and
Datastreams in istSOS4 from an Excel template, one row per sensor. Entities
that already exist with the same name are reused, so re-running a sheet is
safe. With `--observations` it loads observations into those Datastreams
instead, see [Observations](#observations).

The `datastream_begin` column is ignored: istSOS4 computes a Datastream's
`phenomenonTime` from its observations.

## Setup

```bash
cp .env.example .env    # istSOS4 URL and credentials
docker build -t ghcr.io/istsos/istsos4/utils/xlsx2istsos:0.3 .
```

## Run

```bash
./run.sh /absolute/path/to/file.xlsx
./run.sh                                  # uses XLSX_PATH from .env
./run.sh file.xlsx --sheet-name Sheet2 --commit-message "Import 2026 stations"
```

`run.sh` passes `.env` with `--env-file` and mounts the file read-only. The
same call without the wrapper:

```bash
docker run --rm --network host --env-file .env \
  -v /absolute/path/to/file.xlsx:/data/file.xlsx:ro \
  ghcr.io/istsos/istsos4/utils/xlsx2istsos:0.3 /data/file.xlsx
```

## Observations

`--observations` loads observations into Datastreams that already exist:

```bash
./run.sh /absolute/path/to/TIC_7500_20260604040000+0100.xlsx --observations
./run.sh FILE.xlsx --observations --force --qc 10 --commit-message "bulk obs"
```

- The file is named `<thing>_<YYYYMMDDhhmmss±hhmm>.xlsx`: the Thing and the
  time of its last observation. A sheet whose last row is later than that is
  skipped with a warning.
- Each sheet is one Datastream of that Thing, named like the sheet, with the
  columns `time`, `result`, `resultQuality`. Empty sheets and sheets that
  repeat a time are skipped; a time without offset takes the filename's.
- Every sheet is read and matched before anything is posted, so a sheet with
  no Datastream stops the import.
- Without `--force` the server rejects observations already stored. With
  `--force` it first deletes the Datastream's stored observations within the
  sheet's time range, chunk by chunk.
- `--qc N` sets `resultQuality` to `N` (an integer) for every observation,
  ignoring the `resultQuality` column.
- Observations are sent in chunks of at most 2457 rows per request
  (istsos4-client's `MAX_ROWS_PER_BULK`), each with its own commit message:
  `execution time <run time>, <commit-message> for <file>, sheet <sheet> (i/N)`, e.g.
  `execution time 2026-10-09T15:23:09+02:00, bulk obs for TIC_10320_20260922124000+0100.xlsx, sheet Tu_TIC_10320 (1/1)`.
  N counts the chunks of the whole file.
- The filename is read inside the container: `run.sh` mounts the file at its
  own path, keep the name when mounting by hand.
