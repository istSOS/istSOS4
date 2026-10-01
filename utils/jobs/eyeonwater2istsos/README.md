# eyeonwater2istsos

Imports [EyeOnWater](https://www.eyeonwater.org) citizen-science observations
into istSOS4: one Party per contributor, one FeatureOfInterest per photo
location and one Datastream per measured quantity, all linked to an existing
Thing and to a Network created on first use. Re-running is safe: existing
entities are reused and observations already stored (HTTP 409) are skipped.

Needs an istSOS4 build with STAplus (Parties) and istsos4-client >= 0.2.

## Setup

```bash
cp .env.example .env    # credentials, Thing id, Network name, bbox
docker build -t ghcr.io/istsos/istsos4/utils/eyeonwater2istsos:0.1 .
```

## Fetch from the EyeOnWater API

```bash
./fetch.sh                                    # last EYEONWATER_LOOKBACK_DAYS days
BEGIN="2026-06-01T00:00:00" ./fetch.sh        # custom start
BBOX="46.18,6.11,46.54,6.96" ./fetch.sh       # min_lat,min_lon,max_lat,max_lon
```

Daily at 03:15; `fetch.sh` computes `--begin` itself:

```cron
15 3 * * * /path/to/istSOS4/utils/jobs/eyeonwater2istsos/fetch.sh >> /var/log/eyeonwater2istsos.log 2>&1
```

## Import a JSON export

```bash
./run.sh /absolute/path/to/file.json
./run.sh                                      # uses EYEONWATER_JSON_PATH from .env
```

## Test

```bash
python test_eyeonwater2istsos.py
```
