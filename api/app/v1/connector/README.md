# istSOS Metadata Connector

Reads istSOS4 sensor metadata straight from Postgres and republishes it as a STAC 1.0 catalog and a DCAT-AP 3.0 catalog, so istSOS4 deployments can be crawled by STAC clients and listed in EU Data Space catalogs without either ecosystem needing to speak SensorThings.

Built as part of GSoC 2026, under OSGeo/istSOS by Vishmayraj

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

Nothing in this package runs per request except the API layer itself. On a fixed interval (`HARVEST_INTERVAL_MINUTES`, default `5`), APScheduler fires `scheduled_harvest_job()`. That job:

1. Takes a Postgres advisory lock, so two workers never harvest at once.
2. Runs one JOIN query across `Thing`, `Location`, `Datastream`, `ObservedProperty`, and `Sensor` (a second query joins `Network` too, when `NETWORK=1`), and folds the rows into a `HarvestedCatalog` / `HarvestedNetworkCatalog`.
3. Hands that catalog to `stac_transformer.py` and `dcat_transformer.py` independently. Either one can fail without rolling back the other -- a broken DCAT config does not take STAC down with it, and vice versa.
4. Writes the result to Redis via `cache.py`, purging the previous cycle's keys first.

`api.py` never touches Postgres. It reads whatever `cache.py` last wrote and serves it as-is, so if no harvest cycle has completed yet, routes return `503` rather than blocking on a live query.

Both transformers are off by default. Set `STAC_TRANSFORMER=1` and/or `DCAT_TRANSFORMER=1` to turn them on; a disabled standard's routes return `404` rather than being unmounted, so the route table stays predictable.

### NETWORK mode

`NETWORK` (default `0`) controls whether the deployment is flat or scoped by SensorThings `Network`:

- `NETWORK=0`: one flat catalog. `/connector/stac` and `/connector/dcat/root` each serve everything directly.
- `NETWORK=1`: the harvest additionally groups Datastreams by `network_id`.

Datastreams with no `Network` land in an "orphan" scope; everything else gets its own sub-catalog at `/connector/stac/{network_id}` and `/connector/dcat/{network_id}`. On the DCAT side the root graph stays structural (Catalog + DataService + links to the other scopes) rather than carrying Dataset content itself -- fetch `/connector/dcat/orphan` and `/connector/dcat/{network_id}` for the scopes that actually hold data.

### Authorization

`auth_gate.py`'s `gate()` dependency sits in front of every connector route. It has three effective modes:

- `AUTHORIZATION=0`, or `ANONYMOUS_VIEWER=1`: everything is public.
- Strict mode (`AUTHORIZATION=1`, `ANONYMOUS_VIEWER=0`), no closed networks configured: any valid istSOS4 bearer token passes; whether an anonymous request is allowed through comes down to `OPEN_CATALOG_METADATA` (default `1` -- set it to `0` to require a token for every route, not just the deep ones).
- Strict mode with `CATALOG_CLOSED_NETWORKS` set: an unauthenticated request for a listed network id gets `404`, not `401` -- the network's existence itself is hidden, not just its content. `CATALOG_CLOSED_NETWORKS` accepts network ids and/or names (`3,Rain Gauges`); names are resolved to ids once at startup against `sensorthings."Network"`.

A closed network is still built and cached every cycle, and an authenticated caller can always reach it directly by id. What changes is discoverability from the root: `stac_transformer.py` strips a closed network's child link from the cached root Catalog, and `dcat_transformer.py` builds two versions of the root graph for exactly this reason -- a public one with closed networks omitted from `dct:hasPart`/`dcat:catalog`, and an `_all` variant with them included. `api.py` serves the public version to anonymous callers and the `_all` version to authenticated ones, so an authenticated request to `/connector/stac` or `/connector/dcat/root` can discover a closed network exists, even though an anonymous one cannot.

Assets on individual STAC Items and DCAT Distributions (the underlying STA `Observations`/`Datastream` links) carry their own `auth:schemes` declaration whenever the STA API itself requires a token (`AUTHORIZATION=1` and `ANONYMOUS_VIEWER=0`) -- this is a separate concern from the connector's own catalog gating above, since a catalog entry can be openly visible while the data it points at still needs a login.

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
stac:meta:availability / stac:meta:last_fetch
```

DCAT-AP (each scope cached as one whole serialized document, Turtle and JSON-LD side by side under a `:jsonld` sibling key):

```
dcat:graph:root                    structural root (Catalog + DataService), closed networks omitted
dcat:graph:root:all                same root, closed networks included -- authenticated callers only
dcat:graph:orphan                  Datastreams with no assigned Network      (NETWORK=1 only)
dcat:graph:net-{network_id}        one per Network                            (NETWORK=1 only)
dcat:meta:availability / dcat:meta:last_fetch / dcat:meta:network_ids
```

Every write purges the previous cycle's keys (`SCAN` + `DELETE` on the relevant prefix) before writing the new set, so a reader hitting the cache mid-write sees a transient miss rather than a mix of old and new data.

---

## Configuration

Everything below lives in `config.py`, either as a `Settings` (pydantic) field or, for the handful of flags that need to be readable before a request even reaches `Settings`-aware code, a plain module attribute read once off `os.environ` at import time.

| Variable | Default | Description |
|---|---|---|
| `STAC_TRANSFORMER` | `0` | Master switch for the STAC 1.0 side. Routes 404 while off. |
| `DCAT_TRANSFORMER` | `0` | Master switch for the DCAT-AP 3.0 side. Routes 404 while off. |
| `NETWORK` | `0` | Scope harvesting/serving by SensorThings `Network` instead of one flat catalog. |
| `OPEN_CATALOG_METADATA` | `1` | In strict auth mode, whether shallow catalog routes stay public. |
| `CATALOG_CLOSED_NETWORKS` | *(unset)* | Comma-separated network ids and/or names to hide from unauthenticated callers. |
| `STAC_AUTH_DESCRIPTION` | *(a sensible default)* | Text shown in the STAC auth extension's login instructions. |
| `HARVEST_INTERVAL_MINUTES` | `5` | How often `scheduled_harvest_job()` fires. |
| `STAC_CATALOG_ID` | `istsos-connector-catalog` | `id` of the root STAC Catalog. |
| `STAC_CATALOG_TITLE` | *(unset)* | Optional human-readable title for the root STAC Catalog. |
| `STAC_DEPLOYMENT_NAME` | `istSOS4` | Interpolated into the root Catalog's description text. |
| `STAC_DEFAULT_LICENSE` | `proprietary` | Fallback `Collection.license`. Must be an SPDX id, `various`, or `proprietary`. |
| `DCAT_CATALOG_ID` | `istsos-connector-dcat-catalog` | `dct:identifier` of the root `dcat:Catalog`. |
| `DCAT_CATALOG_TITLE` | *(unset, mandatory to set)* | `dct:title` of the root Catalog. Required by DCAT-AP 3.0. |
| `DCAT_CATALOG_DESCRIPTION` | *(unset, mandatory to set)* | `dct:description` of the root Catalog. Required by DCAT-AP 3.0. |
| `DCAT_DEPLOYMENT_NAME` | `istSOS4` | Interpolated into composed `dct:description` text. |
| `DCAT_LANGUAGE` | `en` | BCP-47 tag used on every language-tagged literal. Must resolve via the EU NAL language table. |
| `DCAT_DEFAULT_LICENSE` | *(unset)* | Fallback `dct:license` URI for Datasets/Distributions with no license of their own. |
| `DCAT_DEFAULT_ACCESS_RIGHTS` | *(unset)* | Fallback `dct:accessRights` URI for Datastreams with no access rights of their own. |
| `DCAT_PUBLISHER_NAME` | *(unset, mandatory to set)* | `foaf:name` of the publisher Agent. Required by DCAT-AP 3.0. |
| `DCAT_PUBLISHER_URI` | *(unset)* | URI identifying the publisher Agent. Without it, the publisher is emitted as a blank node. |
| `DCAT_PUBLISHER_HOMEPAGE` | *(unset)* | `foaf:homepage` of the publisher Agent. |
| `DCAT_PUBLISHER_MBOX` | *(unset)* | Publisher contact email. `mailto:` is prepended automatically if missing. |

If `DCAT_TRANSFORMER=1` but `DCAT_CATALOG_TITLE`, `DCAT_CATALOG_DESCRIPTION`, or `DCAT_PUBLISHER_NAME` is left unset, the scheduler logs a warning every cycle and skips the DCAT write entirely -- a partially-mandatory-field DCAT-AP graph is worse than none, so the connector will not cache or serve one.

---

## Documentation

| Reference | Covers |
|---|---|
| [Harvesting Layer](docs/Harvesting-Layer-Reference.md) | Scheduling model, the harvest query (both NETWORK modes), the internal data model, the transformer contract |
| [STA-STAC Mapping](docs/STA-STAC-Transformation-Layer-Reference.md) | Datastream-to-Item and Thing-to-Collection mapping, auth extension wiring, scope handling |
| [STA-DCAT-AP Mapping](docs/STA-DCAT-AP-Transformation-Layer-Reference.md) | DCAT-AP 3.0 mapping, SHACL-driven typing decisions, scope handling |
