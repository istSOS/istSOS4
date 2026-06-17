# istSOS4 Utility Image

This folder contains small import utilities and shell wrappers to run them in
one shared Docker image.

## Folder Structure

```text
utils/
  dockerfile                    Dockerfile for the shared utility image
  .env.example                  Template for runtime configuration
  .env                          Real runtime configuration, ignored by git
  models.py                     Shared helper models
  xlsx2istsos.py                Import sensors/datastreams from Excel
  eyeonwater2istsos.py          Import EyeOnWater observations from JSON
  fetch_eyeonwater2istsos.py    Fetch EyeOnWater API data and import it
  xlsx2istsos.sh                Docker wrapper for xlsx2istsos.py
  eyeonwater2istsos.sh          Docker wrapper for eyeonwater2istsos.py
  fetch_eyeonwater2istsos.sh    Docker wrapper for cron/API fetches
```

The Docker image contains Python, the Python dependencies, and the `utils/*.py`
files copied into `/app`.

The shell wrappers load `utils/.env` and pass it to Docker with `--env-file`.
If `utils/.env` does not exist, they fall back to the project root `.env`.

## Runtime Env File

Create the real env file from the template:

```bash
cp utils/.env.example utils/.env
nano utils/.env
```

The real `utils/.env` should contain the values used by the wrappers and
Python scripts, for example:

```env
ISTSOS4_UTILS_IMAGE=istsos4-utils
DOCKER=/usr/bin/docker

ISTSOS4_URL=http://localhost:8018/istsos4/v1.1
ISTSOS4_USERNAME=admin
ISTSOS4_PASSWORD=admin

EYEONWATER_THING_ID=12
EYEONWATER_NETWORK_NAME=winca4ti
EYEONWATER_BBOX=6.116638,46.187437,6.963959,46.545639
EYEONWATER_LOOKBACK_DAYS=2

XLSX_PATH=/absolute/path/to/file.xlsx
EYEONWATER_JSON_PATH=/absolute/path/to/file.json
```

Do not put real passwords in `.env.example`; keep them in `utils/.env`.

## Build The Image

From the project root:

```bash
docker build -f utils/dockerfile -t istsos4-utils .
```

Rebuild the image whenever a `utils/*.py` file changes, because the Python files
are copied into the image at build time.

## Calls With Shell Wrappers

Fetch EyeOnWater API observations and import them. By default this fetches from
`EYEONWATER_LOOKBACK_DAYS` days ago:

```bash
utils/fetch_eyeonwater2istsos.sh
```

Fetch EyeOnWater with a custom begin date:

```bash
BEGIN="2026-06-01T00:00:00" utils/fetch_eyeonwater2istsos.sh
```

Fetch EyeOnWater with a custom bbox:

```bash
BBOX="6.116638,46.187437,6.963959,46.545639" utils/fetch_eyeonwater2istsos.sh
```

Import sensors/datastreams from Excel using `XLSX_PATH` from `utils/.env`:

```bash
utils/xlsx2istsos.sh
```

Import sensors/datastreams from a specific Excel file:

```bash
utils/xlsx2istsos.sh /absolute/path/to/file.xlsx
```

Import EyeOnWater observations from JSON using `EYEONWATER_JSON_PATH` from
`utils/.env`:

```bash
utils/eyeonwater2istsos.sh
```

Import EyeOnWater observations from a specific JSON file:

```bash
utils/eyeonwater2istsos.sh /absolute/path/to/file.json
```

## Calls With Docker Directly

Show help for each program:

```bash
docker run --rm --network host --env-file utils/.env istsos4-utils /app/fetch_eyeonwater2istsos.py --help
docker run --rm --network host --env-file utils/.env istsos4-utils /app/xlsx2istsos.py --help
docker run --rm --network host --env-file utils/.env istsos4-utils /app/eyeonwater2istsos.py --help
```

First full EyeOnWater API import, without a `begin` filter:

```bash
docker run --rm --network host --env-file utils/.env \
  istsos4-utils \
  /app/fetch_eyeonwater2istsos.py \
  --bbox "6.116638,46.187437,6.963959,46.545639"
```

Import Excel directly with Docker:

```bash
docker run --rm --network host --env-file utils/.env \
  -v /absolute/path/to/file.xlsx:/absolute/path/to/file.xlsx:ro \
  istsos4-utils \
  /app/xlsx2istsos.py \
  /absolute/path/to/file.xlsx
```

Import EyeOnWater JSON directly with Docker:

```bash
docker run --rm --network host --env-file utils/.env \
  -v /absolute/path/to/file.json:/absolute/path/to/file.json:ro \
  istsos4-utils \
  /app/eyeonwater2istsos.py \
  /absolute/path/to/file.json
```

## Cron

Daily EyeOnWater update at 03:15:

```cron
15 3 * * * /home/ist/Documents/code/istSOS4/utils/fetch_eyeonwater2istsos.sh >> /home/ist/eyeonwater2istsos.log 2>&1
```

The wrapper computes `--begin` from `EYEONWATER_LOOKBACK_DAYS`, so the cron line
does not need an inline `date` command.

## Moving To A Server

Recommended approach:

```bash
git pull
docker build -f utils/dockerfile -t istsos4-utils .
cp utils/.env.example utils/.env
nano utils/.env
utils/fetch_eyeonwater2istsos.sh --help
```

If you build the image on another machine, export and import it:

```bash
docker save istsos4-utils -o istsos4-utils.tar
scp istsos4-utils.tar user@server:/tmp/
docker load -i /tmp/istsos4-utils.tar
```

Then copy the shell wrappers and `utils/.env` to the server in the same
`utils/` folder layout.
