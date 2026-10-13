# istSOS Metadata Connector

Reads istSOS4 sensor metadata straight from Postgres and republishes it as a STAC 1.0 catalog and a DCAT-AP 3.0 catalog, so istSOS4 deployments can be crawled by STAC clients and listed in EU Data Space catalogs without either ecosystem needing to speak SensorThings.

Built as part of GSoC 2026, under OSGeo/istSOS by Vishmayraj

---

## Quick start

In `.env`, turn on one or both standards (the DCAT values below are the three DCAT-AP mandatory fields):

```
STAC_TRANSFORMER=1
DCAT_TRANSFORMER=1
DCAT_CATALOG_TITLE=My istSOS4 Catalog
DCAT_CATALOG_DESCRIPTION=Sensor metadata from my deployment
DCAT_PUBLISHER_NAME=My Organization
```

Start the stack from this checkout (the published `docker-compose.yml` images only include the connector once a release ships it):

```bash
docker compose -f dev_docker-compose.yml up -d --build
```

The first harvest runs as soon as the API starts. Check it:

```bash
curl http://localhost:8018/istsos4/v1.1/connector
```

`stac.available` and `dcat.available` turn `true` once the first cycle has written the cache. Point any STAC client (e.g. [STAC Browser](https://radiantearth.github.io/stac-browser/)) at `/connector/stac`.

---

## Endpoints

All paths are relative to `{HOSTNAME}{SUBPATH}{VERSION}/connector`, i.e. `http://localhost:8018/istsos4/v1.1/connector` with the defaults.

| Path | Returns |
|---|---|
| `/` | Summary: which standards are on, cache availability, last harvest time, Network ids |
| `/stac` | Root STAC Catalog (also the STAC API landing page) |
| `/stac/conformance` | STAC API conformance classes |
| `/stac/collections` | All Collections, one per Thing (`NETWORK=0`) |
| `/stac/collections/{thing-id}` | One Collection |
| `/stac/collections/{thing-id}/items` | Its Items as a GeoJSON FeatureCollection, one per Datastream |
| `/stac/collections/{thing-id}/items/{datastream-id}` | One Item |
| `/stac/{network_id}` | Network subcatalog (`NETWORK=1`) |
| `/stac/{network_id}/collections[/...]` | Same Collection/Item routes as above, scoped to one Network |
| `/dcat/root`, `/dcat/root.ttl` | Root DCAT-AP Catalog as JSON-LD / Turtle |
| `/dcat/{network_id}`, `/dcat/{network_id}.ttl` | One Network's DCAT-AP sub-catalog (`NETWORK=1`) |

Ids are `thing-{id}` for Collections and `datastream-{id}` for Items. Every route reads from the cache: `503` means no harvest cycle has finished yet, `404` means the scope doesn't exist or the standard is switched off.

---

## Package layout

```
v1/connector/
    api.py               # APIRouter -- every /connector/stac/... and /connector/dcat/... route
    auth_gate.py          # gate() dependency -- shallow/deep tier auth, closed-network 404s
    harvester.py          # asyncpg JOIN query(ies), HarvestedCatalog / HarvestedNetworkCatalog
    scheduler.py          # scheduled_harvest_job() -- advisory lock, runs both transformers, writes Redis
    stac_transformer.py   # HarvestedCatalog -> STAC 1.0 Catalog / Collection / Item dicts
    dcat_transformer.py   # HarvestedCatalog -> DCAT-AP 3.0 rdflib.Graph objects
    cache.py               # Redis read/write helpers -- the only module that touches Redis
    config.py             # Settings (pydantic) + the handful of flags read straight off os.environ
    exceptions.py         # HarvesterError hierarchy
    utils.py               # shared STA parsing helpers, cache-key flattening, HTTP error envelope
    docs/
        Harvesting-Layer-Reference.md
        STA-STAC-Transformation-Layer-Reference.md
        STA-DCAT-AP-Transformation-Layer-Reference.md
```

---

## How it works

Nothing in this package runs per request except the API layer itself. On a fixed interval (`HARVEST_INTERVAL_MINUTES`, default `15`), APScheduler fires `scheduled_harvest_job()`. That job:

1. Takes a Postgres advisory lock, so two workers never harvest at once.
2. Runs one JOIN query across `Thing`, `Location`, `Datastream`, `ObservedProperty`, and `Sensor` (joining `Network` too when `NETWORK=1`), and folds the rows into a `HarvestedCatalog` / `HarvestedNetworkCatalog`.
3. Hands that catalog to `stac_transformer.py` and `dcat_transformer.py` independently. Either one can fail without rolling back the other -- a broken DCAT config does not take STAC down with it, and vice versa.
4. Writes the result to Redis via `cache.py`, purging the previous cycle's keys first.

`api.py` never touches Postgres. It reads whatever `cache.py` last wrote and serves it as-is, so if no harvest cycle has completed yet, routes return `503` rather than blocking on a live query.

Both transformers are off by default. Set `STAC_TRANSFORMER=1` and/or `DCAT_TRANSFORMER=1` to turn them on; a disabled standard's routes return `404` rather than being unmounted, so the route table stays predictable.

### NETWORK mode

`NETWORK` (default `0`) controls whether the deployment is flat or scoped by SensorThings `Network`:

- `NETWORK=0`: one flat catalog. `/connector/stac` and `/connector/dcat/root` each serve everything directly.
- `NETWORK=1`: the harvest additionally groups Datastreams by `network_id`.

Every Datastream is expected to carry a `network_id` once `NETWORK=1` is enabled -- there is no orphan/unassigned scope. Each Network gets its own sub-catalog at `/connector/stac/{network_id}` and `/connector/dcat/{network_id}`. On the DCAT side the root graph stays structural (Catalog + DataService + links to the Network sub-catalogs) rather than carrying Dataset content itself -- fetch `/connector/dcat/{network_id}` for the scopes that actually hold data.

### Authorization

`auth_gate.py`'s `gate()` dependency sits in front of every connector route. It has three effective modes:

- `AUTHORIZATION=0`, or `ANONYMOUS_VIEWER=1`: everything is public.
- Strict mode (`AUTHORIZATION=1`, `ANONYMOUS_VIEWER=0`), no closed networks configured: any valid istSOS4 bearer token passes; whether an anonymous request is allowed through comes down to `OPEN_CATALOG_METADATA` (default `1` -- set it to `0` to require a token for every route, not just the deep ones).
- Strict mode with `CATALOG_CLOSED_NETWORKS` set: an unauthenticated request for a listed network id gets `404`, not `401` -- the network's existence itself is hidden, not just its content. `CATALOG_CLOSED_NETWORKS` accepts network ids and/or names (`3,Rain Gauges`); names are resolved to ids once at startup against `sensorthings."Network"`. Outside strict mode there is nobody to hide a Network from (or nobody who could log in to reveal it), so the setting is ignored with a startup warning.

A closed network is still built and cached every cycle, and an authenticated caller can always reach it directly by id. What changes is discoverability from the root: `stac_transformer.py` strips a closed network's child link from the cached root Catalog, and `dcat_transformer.py` builds two versions of the root graph for exactly this reason -- a public one with closed networks omitted from `dct:hasPart`/`dcat:catalog`, and an `_all` variant with them included. `api.py` serves the public version to anonymous callers and the `_all` version to authenticated ones, so an authenticated request to `/connector/stac` or `/connector/dcat/root` can discover a closed network exists, even though an anonymous one cannot.

Assets on STAC Items (the underlying STA `Observations`/`Datastream` links) carry the STAC [authentication extension](https://github.com/stac-extensions/authentication) (`auth:schemes`, `auth:refs`) whenever the STA API itself requires a token (`AUTHORIZATION=1` and `ANONYMOUS_VIEWER=0`) -- this is a separate concern from the connector's own catalog gating above, since a catalog entry can be openly visible while the data it points at still needs a login.

---

## Redis cache scheme

`cache.py` is the only module that reads or writes Redis, and every key is plain, flat, and namespaced by scope. Nothing is nested; a Collection or Item can be fetched by key directly, without loading or parsing the whole tree.

STAC:

```
stac:catalog                              root Catalog
stac:collection:{collection_id}            one Collection (collection_id = "thing-{id}")
stac:item:{collection_id}:{item_id}         one Item        (item_id = "datastream-{id}")
stac:network:{network_id}                   Network subcatalog          (NETWORK=1 only)
stac:network:{network_id}:collection:{cid}  Network-scoped Collection   (NETWORK=1 only)
stac:network:{network_id}:item:{cid}:{iid}  Network-scoped Item         (NETWORK=1 only)
stac:meta:availability / stac:meta:last_harvested_at
```

DCAT-AP (each scope cached as one whole serialized document; Turtle under the key below, JSON-LD under the same key plus `:jsonld`):

```
dcat:graph:root                    root Catalog + DataService (NETWORK=0: also every Dataset; NETWORK=1: structural only, closed networks omitted)
dcat:graph:root:all                same root, closed networks included -- authenticated callers only (NETWORK=1 only)
dcat:graph:net-{network_id}        one per Network                            (NETWORK=1 only)
dcat:meta:availability / dcat:meta:last_harvested_at
```

Network ids are tracked separately from either standard, since the same Networks back both STAC's subcatalogs and DCAT's Network graphs:

```
{CONNECTOR_KEY_PREFIX}:meta:network_ids   all Network ids seen by the last harvest cycle (NETWORK=1 only)
```

These are logical key names. In Redis each standard is published as a whole *generation*: every key is stored as `{CONNECTOR_KEY_PREFIX}:{stac|dcat}:{generation}:{logical key}`, and `{CONNECTOR_KEY_PREFIX}:{stac|dcat}:current` names the generation being served. A cycle serializes everything first, writes the new generation under fresh keys, and only then flips `current` in one MULTI/EXEC. A reader therefore never sees a gap or a half-written catalog, and if serialization or the write fails the pointer is not moved and the previous catalog keeps being served. The previous generation is kept for one more cycle for readers that resolved the old pointer just before the flip; older ones are deleted. Redis therefore holds up to two generations of each standard.

`CONNECTOR_KEY_PREFIX` defaults to `connector:` plus the deployment's `HOSTNAME`+`SUBPATH`+`VERSION`, so several istSOS4 deployments can share one Redis without serving each other's catalogs. The Redis host and port come from `REDIS_HOST` (default `redis`) and `REDIS_PORT` (default `6379`).

---

## Configuration

Everything below lives in `config.py`, either as a `Settings` (pydantic) field or, for the handful of flags that need to be readable before a request even reaches `Settings`-aware code, a plain module attribute read once off `os.environ` at import time.

| Variable | Default | Description |
|---|---|---|
| `STAC_TRANSFORMER` | `0` | Master switch for the STAC 1.0 side. Routes 404 while off. |
| `DCAT_TRANSFORMER` | `0` | Master switch for the DCAT-AP 3.0 side. Routes 404 while off. |
| `NETWORK` | `0` | Scope harvesting/serving by SensorThings `Network` instead of one flat catalog. |
| `CONNECTOR_KEY_PREFIX` | derived from `HOSTNAME`+`SUBPATH`+`VERSION` | Namespace for every Redis key the connector writes, so deployments sharing one Redis stay separate. |
| `REDIS_HOST` / `REDIS_PORT` | `redis` / `6379` | Redis server used by istSOS4 and the connector cache. |
| `OPEN_CATALOG_METADATA` | `1` | In strict auth mode, whether shallow catalog routes stay public. |
| `CATALOG_CLOSED_NETWORKS` | *(unset)* | Comma-separated network ids and/or names to hide from unauthenticated callers. |
| `STAC_AUTH_DESCRIPTION` | *(a sensible default)* | Text shown in the STAC auth extension's login instructions. |
| `HARVEST_INTERVAL_MINUTES` | `15` | How often `scheduled_harvest_job()` fires. The first cycle runs at startup. |
| `LOG_LEVEL` | `INFO` | Python logging level for the API; harvest timings are logged at `INFO`. |
| `STAC_CATALOG_ID` | `istsos-connector-catalog` | `id` of the root STAC Catalog. |
| `STAC_CATALOG_TITLE` | *(unset)* | Optional human-readable title for the root STAC Catalog. |
| `STAC_DEPLOYMENT_NAME` | `istSOS4` | Interpolated into the root Catalog's description text. |
| `STAC_DEFAULT_LICENSE` | `proprietary` | Fallback `Collection.license`. Must be an SPDX id, `various`, or `proprietary`. |
| `DCAT_CATALOG_ID` | `istsos-connector-dcat-catalog` | `dct:identifier` of the root `dcat:Catalog`. |
| `DCAT_CATALOG_TITLE` | *(unset, mandatory to set)* | `dct:title` of the root Catalog. Required by DCAT-AP 3.0. |
| `DCAT_CATALOG_DESCRIPTION` | *(unset, mandatory to set)* | `dct:description` of the root Catalog. Required by DCAT-AP 3.0. |
| `DCAT_DEPLOYMENT_NAME` | `istSOS4` | Interpolated into composed `dct:description` text. |
| `DCAT_LANGUAGE` | `en` | BCP-47 tag used on every language-tagged literal. Must resolve via the EU NAL language table. |
| `DCAT_DEFAULT_LICENSE` | *(unset)* | Fallback `dct:license` for Datasets/Distributions with no license of their own: an SPDX id (e.g. `CC-BY-4.0`) or a license URI. |
| `DCAT_DEFAULT_ACCESS_RIGHTS` | *(unset)* | Fallback `dct:accessRights` URI for Datastreams with no access rights of their own. |
| `DCAT_PUBLISHER_NAME` | *(unset, mandatory to set)* | `foaf:name` of the publisher Agent. Required by DCAT-AP 3.0. |
| `DCAT_PUBLISHER_URI` | *(unset)* | URI identifying the publisher Agent. Without it, the publisher is emitted as a blank node. |
| `DCAT_PUBLISHER_HOMEPAGE` | *(unset)* | `foaf:homepage` of the publisher Agent. |
| `DCAT_PUBLISHER_MBOX` | *(unset)* | Publisher contact email. `mailto:` is prepended automatically if missing. |

If `DCAT_TRANSFORMER=1` but `DCAT_CATALOG_TITLE`, `DCAT_CATALOG_DESCRIPTION`, or `DCAT_PUBLISHER_NAME` is left unset, the scheduler logs a warning naming the missing fields and skips the DCAT write on every cycle -- a DCAT-AP graph missing mandatory fields is worse than none, so the connector will not cache or serve one. `/connector` reports this as `dcat.configured: false`.

Per-entity metadata is picked up from the SensorThings `properties` JSON where present: a Thing's `license`/`providers` on its STAC Collection, and a Datastream's `license`, `accessRights`, `contactPoint` (`{"fn", "email"}`) and `keywords` on its DCAT Dataset.

With several uvicorn workers (as in `docker-compose.yml`), every worker runs its own scheduler; the advisory lock makes all but one skip each cycle.

---

## Tests

See [tests/connector/README.md](../../../../tests/connector/README.md): unit tests run with no stack, and `tests/connector/validate` checks a running connector against the STAC 1.0 JSON schemas and the official DCAT-AP 3.0 SHACL shapes.

---

## Documentation

| Reference | Covers |
|---|---|
| [Harvesting Layer](docs/Harvesting-Layer-Reference.md) | Scheduling model, the harvest query (both NETWORK modes), the internal data model, the transformer contract |
| [STA-STAC Mapping](docs/STA-STAC-Transformation-Layer-Reference.md) | Datastream-to-Item and Thing-to-Collection mapping, auth extension wiring, scope handling |
| [STA-DCAT-AP Mapping](docs/STA-DCAT-AP-Transformation-Layer-Reference.md) | DCAT-AP 3.0 mapping, SHACL-driven typing decisions, scope handling |
