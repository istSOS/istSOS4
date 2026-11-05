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
mkdir -p config && cp config.example.yaml config/
```

`.env` and `config/` stay local (ignored by git) and are never copied into
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
IMPORT_QI=false
```

`IMPORT_NODATA` decides whether "no data" observations are imported too: `true`
(default) imports them, `false` drops them. `NODATA_VALUE` is the sentinel value
to drop (default `-999.9`, compared numerically) and is only used when
`IMPORT_NODATA=false`. Empty and `NaN` istSOS2 results are stored as
`NODATA_VALUE` (istSOS4 rejects a null result), so `IMPORT_NODATA=false` drops
them too; aggregated jobs leave them out of the average.

`IMPORT_QI=true` copies the istSOS2 quality index (e.g. `210`) as-is into
`resultQuality`, except `130` → `132` and `200` → `208`: those two are also
valid istSOS4 bitmasks; `false` (default) sets `resultQuality` to 3 (data present, no
checks).

Jobs live in `config/`, one YAML file per istSOS2 service (e.g.
`config/sosraw.yaml`, `config/sos.yaml`; `config.example.yaml` has one example
job per service type); every `config/*.yaml` is loaded, in
file-name order. `continue_on_error` applies to the jobs of its own file.
`datastreams` maps each istSOS2
`PROCEDURE.observed-property` to the istSOS4 datastream name; comment a line to
skip that pair:

```yaml
istsos:
  continue_on_error: true
  imports:
    - name: example
      enabled: true
      service: sosraw
      step_days: 1
      datastreams:
        SOURCE_PROCEDURE.observed-property: TARGET_DATASTREAM
```

`step_days` sets the time window read from istSOS2 per request; tune it to the
data frequency (small values for high-frequency procedures, larger ones for
sparse data). It is independent of writing: the client always splits uploads to
istSOS4 into batches that stay below the target database's parameter limit.

## Build

```bash
docker build -t ghcr.io/istsos/istsos4/utils/istsos2-to-istsos4:0.1 .
```

## Push

```bash
docker push ghcr.io/istsos/istsos4/utils/istsos2-to-istsos4:0.1
```

## Run

`config/` is mounted at run time, so editing it needs no rebuild.

```bash
docker run --rm \
  --network host \
  --env-file .env \
  -v "$PWD/config:/app/config:ro" \
  ghcr.io/istsos/istsos4/utils/istsos2-to-istsos4:0.1
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

The script reads `.env` and `config/` from its own directory:

```bash
python3 istsos2_to_istsos4.py
```
