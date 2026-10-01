# istsos4_to_istsos4

Copies observations between two istSOS4 instances, through
[istsos4-client](https://github.com/istSOS/istSOS4-client).

## Requirements

- Docker;
- network access to the source and target instances;
- target datastreams already existing in istSOS4.

Run every command below from this directory:

```bash
cd utils/jobs/istsos4_to_istsos4
cp .env.example .env
```

`.env` holds credentials, stays local (ignored by git) and is never copied into
the image.

## Configuration

Fill in `.env`:

```dotenv
ISTSOS4_FROM_URL=https://source.example/v1.1
ISTSOS4_FROM_USER=source-user
ISTSOS4_FROM_PASSWORD=source-password
NETWORK_FROM=
DATASTREAMS_FROM=
TIMESTAMP_START_FROM=
TIMESTAMP_END_FROM=

ISTSOS4_TO_URL=http://localhost:8019/v4/v1.1
ISTSOS4_TO_USER=target-user
ISTSOS4_TO_PASSWORD=target-password
NETWORK_TO=
DATASTREAMS_TO=

IMPORT_NODATA=true
NODATA_VALUE=-999.9
```

The filters are optional:

- `NETWORK_FROM`: only datastreams of this source network;
- `DATASTREAMS_FROM`: comma-separated source datastream names;
- `TIMESTAMP_START_FROM`: inclusive ISO 8601 lower bound;
- `TIMESTAMP_END_FROM`: inclusive ISO 8601 upper bound;
- `NETWORK_TO`: network in which to look for the target datastreams;
- `DATASTREAMS_TO`: comma-separated target datastream names, paired by
  position with `DATASTREAMS_FROM`. When empty, target names equal the source
  names;
- `IMPORT_NODATA`: `true` (default) also imports "no data" observations,
  `false` drops them;
- `NODATA_VALUE`: sentinel value to drop (default `-999.9`, compared
  numerically), only used when `IMPORT_NODATA=false`.

To copy datastreams whose names differ between the two instances:

```dotenv
DATASTREAMS_FROM=source_temperature,source_rain
DATASTREAMS_TO=target_temperature,target_rain
```

Both lists must contain the same number of names.

Writing to istSOS4 needs no tuning: the client splits every bulk into requests
that stay below the target database's parameter limit.

With empty timestamps every observation is read. The migration processes
observations in blocks the size of one insert and, per block, runs a single
duplicate check that skips `phenomenonTime` values already in the target.

## Build

```bash
docker build -t istsos4-to-istsos4:local .
```

## Run

```bash
docker run --rm \
  --network host \
  --env-file .env \
  istsos4-to-istsos4:local
```

## Logs

The `logging` module prints timestamp and level. Set the verbosity with
`LOG_LEVEL` (`DEBUG`, `INFO` by default, `WARNING`, `ERROR`). At `DEBUG` level it
also shows HTTP request details and authentication events (token refresh,
re-login after a `401`, splitting of an oversized bulk).

## Networking

On Linux, `--network host` lets the container reach services configured as
`localhost`, for example `http://localhost:8019`.

On Docker Desktop use `host.docker.internal` in the URLs instead of
`localhost`; `--network host` can then be dropped.

## Running without Docker

The script reads `.env` from its own directory:

```bash
python3 istsos4_to_istsos4.py
```
