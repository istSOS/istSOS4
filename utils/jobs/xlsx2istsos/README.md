# xlsx2istsos

Creates Locations, Things, Sensors, ObservedProperties, Networks and
Datastreams in istSOS4 from an Excel template, one row per sensor. Entities
that already exist with the same name are reused, so re-running a sheet is
safe.

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
