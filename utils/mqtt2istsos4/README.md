# MQTT2istSOS4

Small service that listens to MQTT messages and inserts them as istSOS4 /
SensorThings Observations.

The app is intentionally narrow:

1. Subscribe to MQTT topics.
2. Read comma-separated MQTT payloads.
3. Match each topic to an ordered list of istSOS datastream names.
4. Resolve datastream names to `@iot.id`.
5. Insert one Observation per valid value.

## Message Format

Expected MQTT payload:

```text
timestamp,value1,value2,value3,...
```

Example:

```text
2026-06-18T14:35:04Z,24.05,119.43,10.04,23.82
```

The first field is used as `phenomenonTime`.

`resultTime` is set by the app when the message is processed.

Every following field is mapped by position:

```text
value1 -> datastream 1
value2 -> datastream 2
value3 -> datastream 3
```

If a message has more values than configured datastreams, the extra values are
ignored. Blank, `null`, `none`, `nan`, `na`, and `n/a` values are skipped.
Non-numeric strings such as `error` are skipped with a warning, and the rest of
the message continues.

## Configuration

Copy the example YAML file:

```bash
cp config.example.yaml config.yaml
```

The app reads `config.yaml` by default. To use another path:

```bash
MQTT2ISTSOS_CONFIG=/path/to/config.yaml python mqtt2istsos.py
```

The old `.env` format is not read anymore; move those values into
`config.yaml`.

### YAML Fields

```yaml
log_level: INFO
dry_run: true

mqtt:
  host: localhost
  port: 1883
  client_id: mqtt2istsos
  username:
  password:
  tls: false
  tls_insecure: false
  keepalive: 60
  qos: 0
  reconnect_delay_sec: 10
  queue_maxsize: 1000
  topics: []

istsos:
  url: http://localhost:8018/v4/v1.1
  username:
  password:
  timeout_sec: 15
  commit_message: mqtt2istsos observation import

mapping:
  path/to/topic:
    - DatastreamA
    - DatastreamB
```

`mqtt.host` / `mqtt.port`: MQTT broker address.

`mqtt.client_id`: MQTT client identifier. Use a unique value if multiple
instances connect to the same broker.

`mqtt.topics`: optional subscription list. If empty or omitted, the app
subscribes to every key in `mapping`.

`mapping`: topic-to-datastream mapping. The datastream order must match the
order of values in the MQTT payload after the timestamp.

`dry_run`: when `true`, the app connects to MQTT, parses messages, and logs the
Observations it would create without logging in to istSOS4 or inserting data.

## istSOS4 Calls

When `dry_run: false`, the app uses the istSOS client to:

1. Authenticate with `POST /Login`.
2. Resolve datastream names with `GET /Datastreams?$filter=name eq ...`.
3. Insert observations with `POST /Observations`.

Observation payloads contain:

```text
Datastream.@iot.id
phenomenonTime
resultTime
result
resultQuality
```

`resultQuality` is set to `"11"` by default.

Datastream IDs are cached after the first successful lookup.

## Run Locally

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
python mqtt2istsos.py
```

## Run With Docker Compose

Build the normal image:

```bash
docker build -t mqtt2istsos:0.1 .
```

Start the service:

```bash
docker compose up -d
```

Check logs:

```bash
docker compose logs -f mqtt2istsos
```

Stop it:

```bash
docker compose down
```

The default compose file mounts `./config.yaml` into the container. Changing
`config.yaml` does not require rebuilding the image, but you should restart the
service:

```bash
docker compose restart mqtt2istsos
```

If both MQTT and istSOS4 are reachable through real hostnames/IPs, you do not
need `network_mode: host`.

Enable `network_mode: host` in `docker-compose.yml` only when the container must
reach services on the host machine through `localhost`, for example
`istsos.url: http://localhost:8018/...` on Linux.

The compose service and normal container are named `mqtt2istsos`. The dev
container is named `mqtt2istsos-dev`.

## Development Compose

Use the dev compose file when changing code:

```bash
docker compose -f docker-compose.dev.yml up --build
```

The dev service bind-mounts the project directory into `/app`, so code changes
are visible inside the container. Restart the service after editing Python code:

```bash
docker compose -f docker-compose.dev.yml restart mqtt2istsos
```

## Logs

Set log verbosity in `config.yaml`:

```yaml
log_level: WARNING
```

Useful values:

```text
DEBUG    very verbose
INFO     startup, subscribe, received-message, and dry-run logs
WARNING  hides INFO logs; shows warnings, errors, and successful inserts
ERROR    only errors
```

Warnings are yellow, errors are red, successful inserts are green, and info logs
are white. Startup/config notice logs are white and visible at every level.

## Debugging

No messages after subscribe acknowledgement:

- Check that MQTT Explorer is showing live messages, not only retained values.
- Temporarily set `mqtt.topics` to `["#"]`.
- Restart the app and look for `Received MQTT message: topic=...`.
- Copy the exact logged topic into `mapping`.

Messages arrive but are not processed:

- The log will say `No datastream mapping for MQTT topic ...`.
- Add that exact topic as a key in `mapping`.

Payload parse errors:

- The message must be comma-separated.
- The first field must be an ISO timestamp like `2026-06-18T15:03:01Z`.
- The rest of the fields are numeric values.

istSOS4 insert problems:

- Start with `dry_run: true`.
- When dry-run logs look correct, set `dry_run: false`.
- Check datastream names in `mapping`; the app resolves them by name before
  inserting.

## Dependencies

```text
httpx
paho-mqtt
PyYAML
```

`paho-mqtt` handles MQTT connection/subscription/message callbacks.

`httpx` is used by the iSTSOS client for login, datastream lookup, and
observation insert requests.

`PyYAML` reads `config.yaml`.
