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
docker build -t ghcr.io/istsos/istsos4/utils/xlsx2istsos:0.1 .
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
  ghcr.io/istsos/istsos4/utils/xlsx2istsos:0.1 /data/file.xlsx
```

## Observations

`--observations` loads observations into Datastreams that already exist:

```bash
./run.sh /absolute/path/to/TIC_7500_20260604040000+0100.xlsx --observations
./run.sh FILE.xlsx --observations --force
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
  sheet's time range.
- The filename is read inside the container: `run.sh` mounts the file at its
  own path, keep the name when mounting by hand.
