# istSOS Metadata Connector

Integrates istSOS4 sensor metadata into the STAC and EU Data Spaces ecosystems. The connector reads directly from the istSOS4 Postgres database, transforms metadata into STAC 1.0 and DCAT-AP 3.0, and caches the result in Redis. Endpoints are mounted onto the main istSOS4 FastAPI application.

Part of GSoC 2026, extending the discoverability and interoperability.

---

## Package layout

```
v1/connector/
    api.py               # APIRouter — /stac/... and /dcat/... endpoints
    harvester.py         # asyncpg JOIN query, HarvestedCatalog dataclass
    scheduler.py         # scheduled_harvest_job(), advisory lock, Redis writes
    stac_transformer.py  # HarvestedCatalog -> STAC 1.0 Catalog/Collections/Items
    dcat_transformer.py  # HarvestedCatalog -> DCAT-AP 3.0 JSON-LD
    cache.py             # Redis read helpers for the API layer
    config.py            # Environment variables, including HARVEST_INTERVAL_MINUTES
    exceptions.py
    utils.py
    docs/
        Harvesting-Layer-Reference.md
        STA-STAC-Transformation-Layer-Reference.md
        STA-DCAT-AP-Transformation-Layer-Reference.md
```

---

## How it works

On a fixed interval (`HARVEST_INTERVAL_MINUTES`, default 15), APScheduler fires `scheduled_harvest_job()`. The job acquires a Postgres advisory lock (to prevent races across workers), runs a single JOIN across `Thing`, `Location`, `Datastream`, `ObservedProperty`, and `Sensor`, normalizes the result into a `HarvestedCatalog`, runs both transformers, and writes `stac:catalog` and `dcat:catalog` to Redis. The API layer never touches Postgres and is a pure Redis reader.

If Redis holds no cache yet (first boot, before the first cycle completes), endpoints return `503`.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `HARVEST_INTERVAL_MINUTES` | `15` | How often the harvest job fires |

All other connector config lives in `config.py` alongside existing istSOS4 settings.

---

## Documentation

| Reference | Covers |
|---|---|
| [Harvesting Layer](docs/Harvesting-Layer-Reference.md) | Scheduling model, harvest query, internal data model, transformer contract |
| [STA-STAC Mapping](docs/STA-STAC-Transformation-Layer-Reference.md) | Datastream-to-Item and Thing-to-Collection mapping, pystac class usage, field tiers |
| [STA-DCAT-AP Mapping](docs/STA-DCAT-AP-Transformation-Layer-Reference.md) | DCAT-AP 3.0 mapping, JSON-LD serialization, property coverage |