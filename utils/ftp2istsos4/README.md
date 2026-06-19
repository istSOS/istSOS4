# ftp2istsos4

Import observations from FTP/SFTP sources into an istSOS 4 SensorThings API.

The script reads one or more remote data sources from `config.yaml`, parses the
configured files, resolves istSOS datastreams, and posts observations to the
configured istSOS endpoint.

## Requirements

- Python 3.11 or newer is recommended.
- Network access to the configured FTP/SFTP servers.
- Network access to the configured istSOS 4 API.
- istSOS credentials with permission to read datastreams and create
  observations.

Install Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

1. Edit `config.yaml`.
2. Set the istSOS connection values:

```yaml
istsos_url: "http://localhost:8018/istsos4/v1.1"
istsos_username: "sensor1"
istsos_password: "sensor"
dry_run: true
```

3. Configure at least one item in the `ftps` list.
4. Run a dry run first:

```bash
python3 run.py
```

5. If the output is correct, set `dry_run: false` and run again.

To use a different configuration file:

```bash
python3 run.py --config path/to/config.yaml
```

## Docker Usage

You can also run the importer with Docker using the included wrapper script:

```bash
./docker-run.sh
```

To build the Docker image locally:

```bash
./docker-build.sh
```

The build script tags the image as:

```bash
ghcr.io/istsos/istsos4/utils/ftp2istsos:0.1
```

To push the image after building, first authenticate with GHCR, then run:

```bash
PUSH_IMAGE=1 ./docker-build.sh
```

The script pulls the published Docker image and runs the importer with:

- `config.yaml` mounted read-only at `/config/config.yaml`.
- `logs/` mounted at `/app/logs`.
- `$HOME/.ssh` mounted read-only at the same path inside the container, if it
  exists.
- The container running as your host user, so generated log files are not owned
  by root.

The default Docker command is equivalent to:

```bash
python /app/run.py --config /config/config.yaml
```

You can pass script arguments after `docker-run.sh`:

```bash
./docker-run.sh --help
./docker-run.sh --config /config/config.yaml
```

The wrapper supports these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `IMAGE_NAME` | `ghcr.io/istsos/istsos4/utils/ftp2istsos:0.1` | Docker image name to pull and run. |
| `CONFIG_FILE` | `./config.yaml` | Host config file to mount into the container. |
| `LOG_DIR` | `./logs` | Host directory where logs are written. |
| `NETWORK_MODE` | `host` | Docker network mode. Set to an empty value to use Docker's default bridge network. |
| `PULL_IMAGE` | `1` | Pull the image before running. Set to `0` to skip pulling, useful for cron after the first run. |

The build script supports these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `IMAGE_NAME` | `ghcr.io/istsos/istsos4/utils/ftp2istsos:0.1` | Docker image name to build. |
| `PUSH_IMAGE` | `0` | Push the image after building. Set to `1` to enable. |

Example with a different config file:

```bash
CONFIG_FILE=/path/to/production.yaml ./docker-run.sh
```

Example using a locally built image without pulling:

```bash
./docker-build.sh
PULL_IMAGE=0 ./docker-run.sh
```

Example crontab entry that runs the published image every hour:

```cron
0 * * * * CONFIG_FILE=/home/daniele/local/git/ftp2istsos4/config.yaml LOG_DIR=/home/daniele/local/git/ftp2istsos4/logs PULL_IMAGE=0 /home/daniele/local/git/ftp2istsos4/docker-run.sh >> /home/daniele/local/git/ftp2istsos4/logs/cron.log 2>&1
```

If your istSOS URL points to `localhost` on the host machine, the default
`NETWORK_MODE=host` works on Linux. On Docker Desktop, use an address reachable
from containers, such as `host.docker.internal`, in `config.yaml`.

## What the Script Does

On each run, the script:

1. Loads `config.yaml`.
2. Configures logging.
3. Logs in to istSOS when a supported importer needs to post observations.
4. Loads all istSOS datastreams and matches configured column names to
   datastream names.
5. Processes each configured source in `ftps`.
6. Posts observations to `/Observations` or `/BulkObservations`.
7. Prints a per-source summary.

If a source has a supported `type`, it is imported. If the `type` is missing or
unsupported, the script only lists the remote FTP/SFTP directory.

## Configuration

Top-level configuration keys:

| Key | Required | Description |
| --- | --- | --- |
| `istsos_url` | yes | Base URL of the istSOS API, for example `http://localhost:8018/istsos4/v1.1`. |
| `istsos_username` | yes | istSOS username. |
| `istsos_password` | yes | istSOS password. |
| `dry_run` | no | If `true`, observations are printed but not posted. FTP files are not moved. Default: `false`. |
| `log_file` | no | Main rotating log file. Default: `logs/ftp2istsos4.log`. |
| `log_level` | no | Python logging level. Default: `INFO`. |
| `log_max_bytes` | no | Max size of one log file before rotation. Default: `5242880`. |
| `log_backup_count` | no | Number of rotated log files to keep. Default: `5`. |
| `source_log_dir` | no | Directory for per-source logs. Default: `logs/sources`. |
| `ftps` | yes | List of FTP/SFTP source definitions. |

Common source keys inside `ftps`:

| Key | Required | Description |
| --- | --- | --- |
| `type` | no | Importer type. Supported importers: `ufam`, `vulink-varese`. |
| `host` | yes | FTP/SFTP host. |
| `port` | no | Port. Defaults to `21` for FTP and `22` for SFTP. |
| `username` | yes | FTP/SFTP username. |
| `password` | no | FTP/SFTP password. If omitted, the script prompts for it. |
| `protocol` | no | `ftp` or `sftp`. Default: `ftp`. |
| `remote-dir` | no | Remote directory. `remote_dir` is also accepted. Default: `.`. |
| `timeout` | no | FTP/SFTP timeout in seconds. Default: `30`. |
| `tz` | no | Time zone used for datetimes without timezone info. Default: `UTC`. |

For SFTP, you can authenticate with a private key:

```yaml
key_path: "/home/user/.ssh/id_rsa"
key_passphrase: "optional-passphrase"
```

`key` is also accepted as an alias for `key_path`. If the key needs a
passphrase and `key_passphrase` is missing, the script prompts for it.

## Column Mapping

Every imported file needs a `columns` list. One column must have
`type: datetime`; value columns must reference an istSOS datastream by name or
ID.

Example:

```yaml
columns:
  - idx: 0
    type: datetime
    format: "%Y-%m-%d %H:%M:%S"
  - idx: 1
    type: float
    name: "WaterLevel"
  - idx: 2
    type: float
    datastream_@iot.id: 42
  - idx: 3
    type: float
    "@iot.id": 43
```

Column keys:

| Key | Description |
| --- | --- |
| `idx` | Zero-based column index in the CSV/text row. |
| `type` | `datetime`, `float`, `int`, or `string`. |
| `format` | Optional Python datetime format for datetime columns. |
| `name` | istSOS datastream name. |
| `datastream_name` | Alias for `name`. |
| `@iot.id` | istSOS datastream ID. Quote this key in YAML as `"@iot.id"`. |
| `datastream_@iot.id` | Alias for `@iot.id`. |

The parser automatically detects common CSV delimiters: comma, semicolon, tab,
and similar CSV dialects.

Empty or invalid numeric values are posted as `-999.9` with a quality flag that
marks the value as no data.

## UFAM Importer

Use `type: ufam` for configured CSV files read directly from FTP or SFTP.

Example:

```yaml
ftps:
  - type: ufam
    protocol: sftp
    host: ftp.hydrodata.ch
    port: 22
    username: "YOUR_USERNAME"
    key_path: "/home/user/.ssh/bafu_id_rsa"
    remote-dir: "/CSV"
    tz: "UTC"
    files:
      - file_path: "2074/BAFU_2074_PegelDrucksonde.csv"
        columns:
          - idx: 0
            type: datetime
            format: "%Y-%m-%d %H:%M:%S"
          - idx: 1
            type: float
            name: "dTEST"
```

Notes:

- `files` must contain at least one file mapping.
- `file_path` is relative to `remote-dir` unless it starts with `/`.
- `path` is accepted as an alias for `file_path`.
- In dry-run mode, the remote file content and generated observations are
  printed, but observations are not posted.

## Vulink Varese Importer

Use `type: vulink-varese` for FTP folders containing ZIP files. The importer
looks for ZIP files in `remote-dir`, reads matching members from each ZIP, posts
observations, then moves the ZIP file to an archive folder.

Example:

```yaml
ftps:
  - type: vulink-varese
    protocol: ftp
    host: ftp.example.org
    port: 21
    username: "YOUR_USERNAME"
    password: "YOUR_PASSWORD"
    remote-dir: "/incoming"
    tz: "Europe/Rome"
    sent_dir: "sent"
    error_dir: "error"
    files:
      - filename_suffix: "BoaVarese-MeteoMTX.txt"
        columns:
          - idx: 0
            type: datetime
            format: "%Y-%m-%dT%H:%M:%S%z"
          - idx: 1
            type: float
            name: "AirPressure_VA"
```

Notes:

- Only `.zip` files in the configured FTP directory are processed.
- A ZIP member is processed when its filename ends with one of the configured
  `filename_suffix` values.
- Successfully processed ZIP files are moved to `sent_dir`.
- Failed ZIP files are moved to `error_dir`.
- `sent_dir` defaults to `sent`.
- `error_dir` defaults to `error`.
- In dry-run mode, ZIP files are not moved.

## Dry Runs

Set this in `config.yaml`:

```yaml
dry_run: true
```

Dry-run mode still reads remote files, parses observations, resolves
datastreams, and prints the API payloads. It does not post observations and does
not archive processed FTP files.

Dry runs are the safest way to check:

- FTP/SFTP credentials.
- File paths and ZIP member suffixes.
- Date parsing.
- Datastream names or IDs.
- Observation payload shape.

## Logs

The script writes:

- One main log file, configured by `log_file`.
- One per-source log file in `source_log_dir`.

At the end of a run, the summary prints the source status and the matching
source log path.

Logs rotate according to `log_max_bytes` and `log_backup_count`.

## Duplicate Observations

The script treats duplicate observations as non-fatal when the istSOS API
returns a duplicate-observation error. Duplicates are counted as skipped in the
summary.

For more than five observations, the script uses `/BulkObservations`. If a bulk
request contains existing observations, it retries the batch one observation at
a time to identify and skip duplicates.

## Troubleshooting

`Configuration file not found`

Check the path passed with `--config`, or run from the directory that contains
`config.yaml`.

`Configuration must contain an 'ftps' list`

The root config must contain:

```yaml
ftps:
  - ...
```

`Datastream '...' does not exist in istSOS`

The configured `name` or `datastream_name` does not match any istSOS datastream.
Check the datastream name in istSOS, or use `@iot.id` / `datastream_@iot.id`
instead.

`exactly one datetime column is required`

Each file config must contain exactly one column with `type: datetime`.

`No matching files in zip`

For `vulink-varese`, check that the ZIP contains files whose names end with the
configured `filename_suffix` values.

`unsupported protocol`

Use `protocol: ftp` or `protocol: sftp`.

## Security Notes

- Do not commit real passwords, SSH keys, or private FTP credentials.
- Prefer SSH key authentication for SFTP sources when possible.
- Keep production configurations outside version control, or use a separate
  private config file passed with `--config`.
