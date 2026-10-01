# istsos2_to_istsos4

Imports observations from istSOS2 into datastreams that already exist in
istSOS4. It talks to istSOS4 through
[istsos4-client](https://github.com/istSOS/istSOS4-client);
[`istsos2_client.py`](istsos2_client.py) reads the legacy istSOS2 API.

## Requirements

- Docker;
- network access to the source and target instances;
- target datastreams already existing in istSOS4.

Run every command below from this directory:

```bash
cd utils/jobs/istsos2_to_istsos4
cp .env.example .env
cp config.example.yaml config.yaml
```

`.env` and `config.yaml` stay local (ignored by git) and are never copied into
the image.

## Configuration

Fill in `.env`:

```dotenv
ISTSOS2_URL=https://source.example/istsos2
ISTSOS2_USER=source-user
ISTSOS2_PASSWORD=source-password

ISTSOS4_URL=http://localhost:8019/v4/v1.1
ISTSOS4_USER=target-user
ISTSOS4_PASSWORD=target-password

IMPORT_NODATA=true
NODATA_VALUE=-999.9
```

`IMPORT_NODATA` decides whether "no data" observations are imported too: `true`
(default) imports them, `false` drops them. `NODATA_VALUE` is the sentinel value
to drop (default `-999.9`, compared numerically) and is only used when
`IMPORT_NODATA=false`.

`config.yaml` defines the jobs. Procedures in the two lists are paired by
position:

```yaml
istsos:
  continue_on_error: true
  imports:
    - name: example
      enabled: true
      service: sosraw
      step_days: 1
      procedures_istsos2:
        - SOURCE_PROCEDURE
      procedures_istsos4:
        - TARGET_DATASTREAM
```

`step_days` sets the time window read from istSOS2 per request; tune it to the
data frequency (small values for high-frequency procedures, larger ones for
sparse data). It is independent of writing: the client always splits uploads to
istSOS4 into batches that stay below the target database's parameter limit.

## Build

```bash
docker build -t istsos2-to-istsos4:local .
```

## Run

`config.yaml` is mounted at run time, so editing it needs no rebuild.

```bash
docker run --rm \
  --network host \
  --env-file .env \
  -v "$PWD/config.yaml:/app/config.yaml:ro" \
  istsos2-to-istsos4:local
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

The script reads `.env` and `config.yaml` from its own directory:

```bash
python3 istsos2_to_istsos4.py
```
