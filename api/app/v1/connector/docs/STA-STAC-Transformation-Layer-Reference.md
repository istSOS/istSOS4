# STA to STAC 1.0 Mapping Reference

**Project:** istSOS Metadata Connector for Data Spaces and STAC
**Author:** Zala Vishmayraj
**Status:** Design, pre-implementation
**Scope:** `connector/stac_transformer.py`, `connector/api.py` (STAC endpoints)
**Input:** `HarvestedCatalog` from `connector/harvester.py` via cache
**Output:** STAC 1.0 Catalog, Collections, and Items as `application/json`, served via FastAPI, compatible with the STAC API spec and eoAPI STAC browser
**Library:** `pystac` >= 1.9.0

---

## Design

This section is a self-contained overview of the STAC transformer. It is intended for quick review.

**The pivot decision.** One Datastream maps to one `stac:Item`. One Thing maps to one `stac:Collection`. The STA service root maps to `stac:Catalog`.

The Datastream is the correct Item pivot because it is the only STA entity that simultaneously carries everything a STAC Item requires: identity, description, spatial extent (`observedArea`), temporal extent (`phenomenonTime`), observation type URI, unit of measurement, provenance via Sensor and ObservedProperty, and a stable URL for constructing Assets. The Thing is the correct Collection because it is the natural grouping unit: one physical device groups all its measurement channels. Collection extents derive bottom-up from member Items, which is both correct STAC semantics and correct deployment semantics.

**What the transformer does:** It receives a `HarvestedCatalog` from the cache, constructs a pystac object tree (Catalog, Collections, Items, Assets), calls `normalize_hrefs`, serializes the entire tree to dicts, and returns them. The FastAPI layer returns those dicts directly as JSON. The transformer never touches the STA API directly. pystac is used for all STAC object construction, not manual dict-building, because it validates required fields at construction time, injects `type` and `stac_version` automatically, and manages the `links` array for navigation relations. Serialized dicts, not pystac objects, go into the cache, as pystac objects maintain internal link state that becomes stale.

**Object hierarchy:**
```
pystac.Catalog      (1, root, from STA service root)
  pystac.Collection (1 per Thing)
    pystac.Item     (1 per Datastream)
      pystac.Asset  (3 per Item)
```

**STA entity role summary:**

| STA entity | STAC role | pystac class | Notes |
|---|---|---|---|
| STA service root | `stac:Catalog` | `pystac.Catalog` | Landing page. All mandatory identity fields from external config |
| Thing | `stac:Collection` | `pystac.Collection` | One Collection per Thing. Groups all Datastreams |
| Datastream | `stac:Item` | `pystac.Item` | Pivot. One Item per Datastream |
| Location | `Collection.extent.spatial` + `Item.geometry` fallback | `pystac.SpatialExtent` / GeoJSON dict | Primary spatial source is `Datastream.observedArea`. Location is fallback only |
| ObservedProperty | `Item.properties` fields + `Collection.keywords` | Inline strings | No pystac class |
| Sensor | `Item.properties` fields | Inline strings | No pystac class |
| Observations collection | `stac:Asset` | `pystac.Asset` | Constructed from Datastream URL pattern. No native STA entity |
| HistoricalLocation | not mapped | | Temporal data comes from `Datastream.phenomenonTime` |
| FeatureOfInterest | not mapped | | Per-observation spatial, not needed at catalog level |

**stac:Catalog field mapping** (all identity fields from external config; `thing_count` from `len(catalog.things)`)

Note: the harvester reads Postgres directly via a single JOIN query. There is no HTTP GET to the STA API. All STA entity data arrives as fields on `HarvestedThing` / `HarvestedCatalog` dataclass instances.

Mandatory:

| STAC field | STA source | pystac construction | Notes |
|---|---|---|---|
| `id` | NONE | `pystac.Catalog(id=config.STAC_CATALOG_ID)` | External config. Required |
| `type` | Fixed | Injected by pystac | Always `"Catalog"` |
| `stac_version` | Fixed | Injected by pystac | Always `"1.0.0"` |
| `description` | Derived + external | `config.STAC_CATALOG_DESCRIPTION or f"istSOS4 deployment: {thing_count} Things"` | Config with fallback to harvested Thing count |
| `links` | Derived by pystac | `catalog.normalize_hrefs(config.STAC_ROOT_HREF)` | pystac auto-generates `self`, `root`, one `child` per Collection. Not constructed manually |

Recommended:

| STAC field | STA source | pystac construction | Notes |
|---|---|---|---|
| `title` | NONE | `pystac.Catalog(title=config.STAC_CATALOG_TITLE)` | External config. Omitted if `None` |
| `conformsTo` | Fixed | `catalog.extra_fields["conformsTo"] = [...]` | STAC API Core + OGC API Features URIs. Hard-coded, not derived from STA conformance |
| `stac_extensions` | NONE | `extra_fields={"stac_extensions": [...]}` | Declare extensions used across Collections and Items |

Optional:

| STAC field | STA source | Notes |
|---|---|---|
| `keywords` | Union of `ObservedProperty.name` and `Thing.name` across catalog | Deduplicated union. Added via `extra_fields` |

**stac:Collection field mapping** (`catalog.things[]` -- one Collection per `HarvestedThing`)

Mandatory:

| STAC field | STA source | pystac construction | Notes |
|---|---|---|---|
| `id` | `Thing.@iot.id` | `pystac.Collection(id=f"thing-{thing.id}")` | Prefixed to avoid collision with Item IDs in STAC API search |
| `type` | Fixed | Injected by pystac | Always `"Collection"` |
| `stac_version` | Fixed | Injected by pystac | Always `"1.0.0"` |
| `description` | `Thing.description` | `thing.description or f"STAC Collection for SensorThings Thing: {thing.name}"` | Fallback composes Thing name into a minimal description |
| `extent.spatial` | Derived from Datastream `observedArea` bboxes | `pystac.SpatialExtent(bboxes=[computed_bbox])` | If can't find any bboxes, set it to world bbox [-180.0, -90.0, 180.0, 90.0] |
| `extent.temporal` | Derived from `Datastream.phenomenonTime` across member Items | `pystac.TemporalExtent(intervals=[[start_dt, end_dt]])` | `end_dt` is `None` for live / open-ended deployments |
| `links` | Derived by pystac + manual | `catalog.add_child(collection)` + manual `sta_thing` link | pystac generates `self`, `root`, `parent`, `items` automatically. One `sta_thing` link added per Collection pointing to the constructed STA URI: `f"{base_url}/v1.1/Things({thing.id})"` |

Recommended:

| STAC field | STA source | pystac construction | Notes |
|---|---|---|---|
| `title` | `Thing.name` | `pystac.Collection(title=thing.name or None)` | Direct map. Omitted if `None` |
| `keywords` | `Thing.name` + union of `ObservedProperty.name` across Datastreams | `extra_fields={"keywords": keywords_list}` | `ObservedProperty.name` follows `category:subcategory:phenomenon_id` in dummy data; split on `:` and include each part |
| `extra_fields["thing_id"]` | `Thing.@iot.id` | `extra_fields={"thing_id": thing.id}` | Preserves numeric STA ID for round-trip lookup and STAC filter expressions |
| `extra_fields["thing_properties"]` | `Thing.properties` | `extra_fields={"thing_properties": thing.properties}` | Full STA properties bag passed through |
| `summaries` | Derived from Datastreams | `extra_fields={"summaries": {...}}` | Union of `ObservedProperty.definition` URIs and `unitOfMeasurement.symbol` values across Datastreams |

Optional:

| STAC field | STA source | Notes |
|---|---|---|
| `license` | NONE | `Thing.properties["license"]` if set, else `config.STAC_DEFAULT_LICENSE`. Added via `extra_fields` |
| `providers` | NONE | External config. List of provider objects. Added via `extra_fields` |
| `stac_extensions` | NONE | Declare STAC extensions applied to Items in this Collection. Added via `extra_fields` |

**stac:Item field mapping** (`thing.datastreams[]` -- one Item per Datastream dict in `HarvestedThing`)

Skip condition: if `Datastream.phenomenonTime` is absent or null and `datetime` cannot be constructed, the Item is skipped entirely (WARNING logged). This is the only entity-level skip in the STAC transformer. Null geometry is tolerated; null `datetime` with no `start_datetime` + `end_datetime` fallback is invalid STAC and forces a skip.

Mandatory:

| STAC field | STA source | pystac construction | Notes |
|---|---|---|---|
| `id` | `Datastream.@iot.id` | `pystac.Item(id=f"datastream-{ds['id']}")` | Prefixed for namespace clarity in STAC API search |
| `type` | Fixed | Injected by pystac | Always `"Feature"` |
| `stac_version` | Fixed | Injected by pystac | Always `"1.0.0"` |
| `geometry` | `Datastream.observedArea` | `pystac.Item(geometry=ds["observed_area"])` | Fallback: first `Thing.Location.geometry`. `None` if neither -- Item still emitted with `null` geometry |
| `bbox` | Derived from geometry | `pystac.Item(bbox=_bbox_from_geometry(geometry))` | `[minx, miny, maxx, maxy]`. `None` if geometry is `None` |
| `datetime` | `Datastream.phenomenonTime` end | `pystac.Item(datetime=_compute_item_datetime(ds))` | Set to `phenomenonTime` end for closed streams. Set to `phenomenonTime` start for open/live streams. Skip Item if both absent |
| `links` | Derived by pystac + manual | `collection.add_item(item)` + manual `sta_datastream` link | pystac generates `self`, `root`, `parent`, `collection` automatically. One `sta_datastream` link added per Item pointing to the constructed STA URI: `f"{base_url}/v1.1/Datastreams({ds['id']})"` |
| `assets` | Constructed | `pystac.Asset(href=..., media_type=..., title=..., roles=[...])`; attached via item.add_asset(key, asset) | Minimum 2 Assets required. pystac does not emit `assets` key if dict is empty, which fails STAC validation |
| `properties` | See Recommended below | `pystac.Item(properties={})` then `_populate_item_properties(item, thing, ds)` | Plain Python dict. All properties fields below written directly to `item.properties` |

Recommended (properties fields):

| Property key | STA source | Construction | Notes |
|---|---|---|---|
| `title` | `Thing.name` + `Datastream.name` | `f"{thing.name} - {ds['name']}"` | Composed for uniqueness. Datastream names alone are not guaranteed unique within a deployment |
| `description` | `Datastream.description` | `ds["description"] or ""` | Empty string if absent. Never `None` in output |
| `start_datetime` | `Datastream.phenomenonTime` start | ISO 8601 with UTC timezone | Always set alongside `datetime` to support interval search |
| `end_datetime` | `Datastream.phenomenonTime` end | ISO 8601 with UTC timezone, or `None` | `None` for live/open streams |
| `thing_id` | `Thing.@iot.id` | `thing.id` | For reverse lookup from Item to parent Thing in STAC filter queries |
| `thing_name` | `Thing.name` | `thing.name` | Denormalized to avoid a Collection lookup to identify the station |
| `datastream_id` | `Datastream.@iot.id` | `ds["id"]` | Explicit for STAC filter expressions, redundant with Item `id` |
| `unit_of_measurement` | `Datastream.unitOfMeasurement` | `ds["unit_of_measurement"]` | Full object `{"name": ..., "symbol": ..., "definition": ...}`. `None` if absent |
| `observation_type` | `Datastream.observationType` URI | `ds["observation_type"]` | e.g. `"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"` |
| `observed_property` | `ObservedProperty.name` | `op["name"]` | Via Datastream inline expand |
| `observed_property_id` | `ObservedProperty.@iot.id` | `op["id"]` | For cross-referencing ObservedProperty across Items in STAC search |
| `observed_property_definition` | `ObservedProperty.definition` | `op["definition"]` | Vocabulary reference. In dummy data this is `"{}"`, a string not a URI. Stored as-is |
| `sensor_name` | `Sensor.name` | `sensor["name"]` | Via Datastream inline expand |
| `sensor_id` | `Sensor.@iot.id` | `sensor["id"]` | Numeric STA ID |
| `sensor_metadata` | `Sensor.metadata` | `sensor["metadata"]` | URL to SensorML / instrument datasheet. `None` if absent |

Optional (properties fields, selected):

| Property key | STA source | Notes |
|---|---|---|
| `created` | `Datastream.properties["created"]` | ISO 8601 string. Omit if key absent |
| `updated` | Latest `Observation.resultTime` | Requires an additional Observations request, not harvested. Omit unless `Datastream.properties["updated"]` is set |
| `platform` | `Thing.name` | Alias for EO-style catalogs |
| `instruments` | `Sensor.name` | Single-element array `[sensor["name"]]` for EO Extension |
| `resolution` | `Datastream.properties["resolution"]` | ISO 8601 duration, e.g. `"PT10M"`. From dummy data |
| Any extra key | `Datastream.properties[key]` | Pass through non-reserved keys. Do not pass through `observedArea`, `phenomenonTime`, `@iot.*` |

**stac:Asset field mapping** (constructed from `ds["id"]` -- Asset hrefs are built as `f"{base_url}/v1.1/Datastreams({ds['id']})/Observations"` etc.)

| Asset key | href | media_type | roles | Consumer |
|---|---|---|---|---|
| `observations_json` | `{ds.self_link}/Observations` | `pystac.MediaType.JSON` | `["data"]` | Programmatic STA API consumers, real-time integrations |
| `observations_csv` | `{ds.self_link}/Observations?$resultFormat=CSV` | `"text/csv"` | `["data"]` | Analytical pipelines, GIS tools, bulk export |
| `datastream` | `ds["self_link"]` | `pystac.MediaType.JSON` | `["metadata"]` | Round-trip navigation back to the STA Datastream entity |

Mandatory (per Asset):

| STAC field | Source | pystac construction |
|---|---|---|
| `href` | Constructed URL | `pystac.Asset(href=...)` , must be an absolute URL |
| `type` | Fixed per mode | `pystac.Asset(media_type=...)`, serialized as `"type"` in JSON |

Recommended (per Asset):

| STAC field | Source | Notes |
|---|---|---|
| `title` | `Datastream.name` + mode suffix | Access-mode suffix for disambiguation in STAC browsers |
| `description` | Fixed per mode | Via `pystac.Asset(extra_fields={"description": "..."})`. pystac has no native `description` on Asset |
| `roles` | Fixed per mode | `["data"]` for observation feeds. `["metadata"]` for Datastream self-link |

**Fallback chains:**

Spatial (Item geometry):
1. `Datastream.observedArea` -- preferred; per-variable spatial footprint from the STA entity
2. `Thing.Location.location` -- first Location only; used when `observedArea` is `None`
3. `None` geometry -- Item still emitted as valid GeoJSON Feature with `"geometry": null`; WARNING logged

Spatial (Collection extent):
1. Union bbox of all Item bboxes within the Collection
2. If no Items have geometry: union of `Thing.Location` geometries directly
3. If neither: world bbox `[-180.0, -90.0, 180.0, 90.0]`; WARNING logged

Temporal (Item datetime):
1. `Datastream.phenomenonTime` parsed as ISO 8601 interval `start/end`
2. If `end` is `".."` or absent: `end_datetime = None`; `item.datetime` set to `start`
3. If both absent: Item is skipped entirely and a WARNING is logged, the only entity-level skip in the STAC transformer

Temporal (Collection extent):
1. `min(start_datetime)` across all successfully-built Items
2. `max(end_datetime)` across all Items; `None` if any Item is open-ended
3. If no Items were produced: `[[None, None]]` open interval

---

**Public interface:**
```python
def transform_to_stac(catalog: HarvestedCatalog) -> dict: ...
```
`transform_to_stac()` constructs the full pystac tree, calls `normalize_hrefs`, serializes to dict, and returns a dict with keys `catalog` and `collections`. The `catalog` key holds the root Catalog dict; the `collections` key holds one `Collection.to_dict()` per Thing. `cache.py` calls this function directly; `api.py` reads from cache and serves the appropriate sub-key per endpoint. Raises `ValueError` if `STAC_ROOT_HREF` is unset. Does not touch Postgres, Redis, or the STA HTTP API.