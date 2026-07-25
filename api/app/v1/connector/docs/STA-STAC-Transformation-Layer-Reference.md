# STA to STAC 1.0 Mapping Reference

**Project:** istSOS Metadata Connector for Data Spaces and STAC
**Author:** Zala Vishmayraj
**Status:** Design, in implementation
**Scope:** `connector/stac_transformer.py`, `connector/api.py` (STAC endpoints)
**Input:** `HarvestedCatalog` from `connector/harvester.py` via cache
**Output:** STAC 1.0 Catalog, Collections, and Items as `application/json`, served via FastAPI, compatible with the STAC API spec and eoAPI STAC browser
**Library:** none. Pure Python dict construction, no pystac dependency

---

## Design

This section is a self-contained overview of the STAC transformer. It is intended for quick review.

**The pivot decision.** One Datastream maps to one `stac:Item`. One Thing maps to one `stac:Collection`. The STA service root maps to `stac:Catalog`.

The Datastream is the correct Item pivot because it is the only STA entity that simultaneously carries everything a STAC Item requires: identity, description, spatial extent (`observedArea`), temporal extent (`phenomenonTime`), observation type URI, unit of measurement, provenance via Sensor and ObservedProperty, and a stable URL for constructing Assets. The Thing is the correct Collection because it is the natural grouping unit: one physical device groups all its measurement channels. Collection extents derive bottom-up from member Items, which is both correct STAC semantics and correct deployment semantics.

**What the transformer does:** it receives a `HarvestedCatalog` from the cache and builds STAC Catalog, Collection, and Item structures as plain Python dicts directly, with no intermediate object graph. All required fields (`id`, `type`, `stac_version`, `links`, `extent`, `geometry`, `bbox`, `datetime`, `assets`) are set explicitly as dict keys. All link relations (`self`, `root`, `parent`, `child`, `item`, `collection`) are constructed by hand using `_datastream_href()` and `_thing_href()`, which build absolute URLs from the `HOSTNAME`, `SUBPATH`, and `VERSION` constants in `app/__init__.py`. This replaced an earlier pystac-based implementation: pystac's `add_child()` / `add_item()` run internal catalog validation on every call, which made a full transform of the Fraunhofer dataset (5,610 Things, 22,941 Datastreams) take close to five minutes end to end. Direct dict construction dropped that to sub-second for the same dataset, and gives tighter, explicit control over STAC 1.0 compliance than delegating to pystac's output shape.

**Network scoping.** The connector reads its own `NETWORK` env var at harvest time, independently of any other service reading the same `.env`. This determines which of two harvest queries runs and whether scoping applies at all:

- `NETWORK=0`: the original single-scope query runs, no `Network` join, no scoping logic. `/stac` is the one and only catalog, exactly as in the pre-Network design. This section's "scope" language below collapses to a single implicit scope in this mode.
- `NETWORK=1`: the extended query runs (see the harvester reference doc, this document does not own the SQL). The connector now serves multiple scopes side by side:
  - `/stac/{network_id}` per Network, listing only Things that have at least one Datastream in that Network, with each Collection's Items filtered to that Network's Datastreams only.
  - `/stac` itself becomes the **orphan scope**: Things that have at least one Datastream with `network_id IS NULL`, with each Collection's Items limited to those unassigned Datastreams. Root's `links` still carries one `child` entry per Network subcatalog (`subcatalog-1`, `subcatalog-2`, ...), alongside `self` and `root`, per the structure the mentors specified. The orphan scope is not a special case grafted on top, it is the same grouping logic as a Network scope, just keyed on `network_id IS NULL` instead of a specific id.

`network_id` lives on `Datastream`, never on `Thing`. A Thing has no scope of its own, a Thing's presence in a given scope is entirely a function of which of its Datastreams happen to fall into that scope. One consequence: a **Datastream never appears in more than one scope**, since `network_id` is single-valued per row, it belongs to exactly one Network's bucket or the orphan bucket, never both. Only **Collections** can appear in more than one scope, when a Thing's Datastreams are split across buckets, each scoped appearance is a distinct partial view of that Thing containing only the Items that fall into that scope.

Collection `id` stays `thing-{id}` in every scope it appears in, it does not get a scope suffix. Uniqueness is enforced by href, not by id: `/stac/collections/thing-X` and `/stac/{network_id}/collections/thing-X` are distinct resources at distinct URLs, and nothing flattens every scope into one shared listing where an id collision would matter. This only becomes a problem if a future cross-scope `/stac/search?collections=thing-X` style endpoint gets built that flattens all scopes into one array; not a concern for the current design. Because each scoped Collection variant sees a different Item subset, its `extent.spatial` and `extent.temporal` must be computed per scope from that scope's own Items, not once globally and reused, the same physical Thing can legitimately have a different bbox and time range depending which scope it is viewed through.

Consequence for `cache.py`, noted here since it originates from this design, actual key scheme is owned by the caching layer doc: the existing Redis key pattern `stac:collection:{id}` is not sufficient once the same id can exist in more than one scope simultaneously. The key needs to carry scope, `stac:collection:{scope}:{id}`, where `scope` is `net-{network_id}` for a Network view or a fixed sentinel (for example `unscoped`) for the orphan view. Avoid reusing the literal word `root` for this sentinel, it collides with the unrelated `root` STAC link relation and with "root Catalog" as a concept.

**Object hierarchy:**
```
NETWORK=0:
Catalog dict        (1, root, from STA service root)
  Collection dict    (1 per Thing, all Datastreams)
    Item dict        (1 per Datastream)
      Asset dict     (3 per Item)

NETWORK=1:
Catalog dict         (1, root -- serves the orphan scope directly)
  Catalog dict       (1 per Network, subcatalog, linked as child from root)
    Collection dict  (1 per Thing with >=1 Datastream in that Network)
      Item dict      (1 per Datastream in that Network)
        Asset dict   (3 per Item)
  Collection dict    (1 per Thing with >=1 orphan Datastream)
    Item dict        (1 per orphan Datastream)
      Asset dict     (3 per Item)
```

**STA entity role summary:**

| STA entity | STAC role | Representation | Notes |
|---|---|---|---|
| STA service root | `stac:Catalog` | Python dict | Landing page. All mandatory identity fields from external config |
| Network | `stac:Catalog` (subcatalog) | Python dict | One subcatalog per Network row, only when `NETWORK=1`. Not itself a Collection, it groups Collections |
| Thing | `stac:Collection` | Python dict | One Collection per Thing per scope it appears in. Groups that scope's Datastreams |
| Datastream | `stac:Item` | Python dict | Pivot. One Item per Datastream, in exactly one scope |
| Location | `Collection.extent.spatial` + `Item.geometry` fallback | GeoJSON dict | Primary spatial source is `Datastream.observedArea`. Location is fallback only |
| ObservedProperty | `Item.properties` fields + `Collection.keywords` | Inline strings | No dedicated dict shape, values folded into the parent dict |
| Sensor | `Item.properties` fields | Inline strings | No dedicated dict shape, values folded into the parent dict |
| Observations collection | `stac:Asset` | Python dict | Constructed from Datastream URL pattern. No native STA entity |
| HistoricalLocation | not mapped | | Temporal data comes from `Datastream.phenomenonTime` |
| FeatureOfInterest | not mapped | | Per-observation spatial, not needed at catalog level |

**stac:Catalog field mapping** (all identity fields from external config; `thing_count` from `len(catalog.things)` for the scope being served)

Note: the harvester reads Postgres directly via JOIN queries. There is no HTTP GET to the STA API. All STA entity data arrives as fields on `HarvestedThing` / `HarvestedCatalog` dataclass instances. The exact query shape is owned by the harvester reference doc, not this one.

Mandatory:

| STAC field | STA source | Construction | Notes |
|---|---|---|---|
| `id` | NONE | `{"id": config.STAC_CATALOG_ID}`, or `f"{config.STAC_CATALOG_ID}-net-{network.id}"` for a Network subcatalog | External config, suffixed per Network for subcatalogs |
| `type` | Fixed | `{"type": "Catalog"}` | Always `"Catalog"` |
| `stac_version` | Fixed | `{"stac_version": "1.0.0"}` | Always `"1.0.0"` |
| `description` | Derived + external | `config.STAC_CATALOG_DESCRIPTION or f"istSOS4 deployment: {thing_count} Things"`; for a Network subcatalog, `f"istSOS4 Network: {network.name}"` | Config with fallback to harvested Thing count |
| `links` | Constructed manually | `_build_catalog_links(scope)` | `self`, `root` always. One `child` per Network subcatalog when `NETWORK=1` and this is the root Catalog. One `child` per included Collection otherwise |

Recommended:

| STAC field | STA source | Construction | Notes |
|---|---|---|---|
| `title` | NONE | `{"title": config.STAC_CATALOG_TITLE}`, or the Network name for a subcatalog | External config. Key omitted if `None` |
| `conformsTo` | Fixed | `{"conformsTo": [...]}` | STAC API Core + OGC API Features URIs. Hard-coded, not derived from STA conformance |
| `stac_extensions` | NONE | `{"stac_extensions": [...]}` | Declare extensions used across Collections and Items |

Optional:

| STAC field | STA source | Notes |
|---|---|---|
| `keywords` | Union of `ObservedProperty.name` and `Thing.name` across the scope | Deduplicated union. Added directly as a dict key |

**stac:Collection field mapping** (one Collection dict per `HarvestedThing` per scope it has at least one Item in)

Mandatory:

| STAC field | STA source | Construction | Notes |
|---|---|---|---|
| `id` | `Thing.@iot.id` | `{"id": f"thing-{thing.id}"}` | Same id used in every scope the Thing appears in. Uniqueness is by href, see Design section above |
| `type` | Fixed | `{"type": "Collection"}` | Always `"Collection"` |
| `stac_version` | Fixed | `{"stac_version": "1.0.0"}` | Always `"1.0.0"` |
| `description` | `Thing.description` | `thing.description or f"STAC Collection for SensorThings Thing: {thing.name}"` | Fallback composes Thing name into a minimal description |
| `extent.spatial` | Derived from this scope's Datastream `observedArea` bboxes only | `{"spatial": {"bbox": [computed_bbox]}}` | Computed per scope, not globally. If no bboxes found in this scope, world bbox `[-180.0, -90.0, 180.0, 90.0]` |
| `extent.temporal` | Derived from this scope's `Datastream.phenomenonTime` across this scope's Items only | `{"temporal": {"interval": [[start_dt, end_dt]]}}` | `end_dt` is `None` for live / open-ended deployments |
| `links` | Constructed manually | `_build_collection_links(thing, scope)` | `self`, `root`, `parent` (pointing at the owning Catalog for this scope, root Catalog or the relevant Network subcatalog), `items`. One `sta_thing` link per Collection pointing to the constructed STA URI: `f"{base_url}/v1.1/Things({thing.id})"` |

Recommended:

| STAC field | STA source | Construction | Notes |
|---|---|---|---|
| `title` | `Thing.name` | `{"title": thing.name}` | Direct map. Key omitted if `None` |
| `keywords` | `Thing.name` + union of `ObservedProperty.name` across this scope's Datastreams | `{"keywords": keywords_list}` | `ObservedProperty.name` follows `category:subcategory:phenomenon_id` in dummy data; split on `:` and include each part |
| `thing_id` | `Thing.@iot.id` | `{"thing_id": thing.id}` | Preserves numeric STA ID for round-trip lookup and STAC filter expressions |
| `thing_properties` | `Thing.properties` | `{"thing_properties": thing.properties}` | Full STA properties bag passed through |
| `network_id` | `Datastream.network_id` for this scope | `{"network_id": network.id}` if this is a Network-scoped Collection, omitted for the orphan scope | Only present on Network-scoped variants, lets a consumer identify which scope a given Collection JSON belongs to without parsing the href |
| `summaries` | Derived from this scope's Datastreams | `{"summaries": {...}}` | Union of `ObservedProperty.definition` URIs and `unitOfMeasurement.symbol` values across this scope's Datastreams |

Optional:

| STAC field | STA source | Notes |
|---|---|---|
| `license` | NONE | `Thing.properties["license"]` if set, else `config.STAC_DEFAULT_LICENSE`. Added directly as a dict key |
| `providers` | NONE | External config. List of provider objects. Added directly as a dict key |
| `stac_extensions` | NONE | Declare STAC extensions applied to Items in this Collection. Added directly as a dict key |

**stac:Item field mapping** (one Item dict per Datastream, in exactly one scope)

Skip condition: if `Datastream.phenomenonTime` is absent or null and `datetime` cannot be constructed, the Item is skipped entirely (WARNING logged). This is the only entity-level skip in the STAC transformer. Null geometry is tolerated; null `datetime` with no `start_datetime` + `end_datetime` fallback is invalid STAC and forces a skip.

Mandatory:

| STAC field | STA source | Construction | Notes |
|---|---|---|---|
| `id` | `Datastream.@iot.id` | `{"id": f"datastream-{ds['id']}"}` | Prefixed for namespace clarity in STAC API search |
| `type` | Fixed | `{"type": "Feature"}` | Always `"Feature"` |
| `stac_version` | Fixed | `{"stac_version": "1.0.0"}` | Always `"1.0.0"` |
| `geometry` | `Datastream.observedArea` | `{"geometry": ds["observed_area"]}` | Fallback: first `Thing.Location.geometry`. `None` if neither, Item still emitted with `"geometry": null` |
| `bbox` | Derived from geometry | `{"bbox": _bbox_from_geometry(geometry)}` | `[minx, miny, maxx, maxy]`. `None` if geometry is `None` |
| `datetime` | `Datastream.phenomenonTime` end | `{"datetime": _compute_item_datetime(ds)}` | Set to `phenomenonTime` end for closed streams. Set to `phenomenonTime` start for open/live streams. Skip Item if both absent |
| `links` | Constructed manually | `_build_item_links(thing, ds, scope)` | `self`, `root`, `parent` and `collection` (both pointing at the owning Collection variant for this scope). One `sta_datastream` link per Item pointing to the constructed STA URI: `f"{base_url}/v1.1/Datastreams({ds['id']})"` |
| `assets` | Constructed | dict of asset key to asset dict, see Asset mapping below | Minimum 2 Assets required. An empty `assets` dict fails STAC validation, so this is never left empty |
| `properties` | See Recommended below | `{"properties": {}}` then `_populate_item_properties(item, thing, ds)` | Plain dict. All properties fields below written directly to `item["properties"]` |

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
| `network_id` | `Datastream.network_id` | `ds["network_id"]` | `None` for an orphan-scope Item. Present so an Item is self-describing even outside its scope's href context |
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

**stac:Asset field mapping** (constructed from `ds["id"]`, Asset hrefs are built as `f"{base_url}/v1.1/Datastreams({ds['id']})/Observations"` etc.)

| Asset key | href | media_type | roles | Consumer |
|---|---|---|---|---|
| `observations_json` | `{ds.self_link}/Observations` | `application/json` | `["data"]` | Programmatic STA API consumers, real-time integrations |
| `observations_csv` | `{ds.self_link}/Observations?$resultFormat=CSV` | `text/csv` | `["data"]` | Analytical pipelines, GIS tools, bulk export |
| `datastream` | `ds["self_link"]` | `application/json` | `["metadata"]` | Round-trip navigation back to the STA Datastream entity |

Mandatory (per Asset):

| STAC field | Source | Construction |
|---|---|---|
| `href` | Constructed URL | `{"href": ...}`, must be an absolute URL |
| `type` | Fixed per mode | `{"type": media_type}` |

Recommended (per Asset):

| STAC field | Source | Notes |
|---|---|---|
| `title` | `Datastream.name` + mode suffix | Access-mode suffix for disambiguation in STAC browsers |
| `description` | Fixed per mode | `{"description": "..."}` |
| `roles` | Fixed per mode | `["data"]` for observation feeds. `["metadata"]` for Datastream self-link |

**Fallback chains:**

Spatial (Item geometry):
1. `Datastream.observedArea`, preferred, per-variable spatial footprint from the STA entity
2. `Thing.Location.location`, first Location only, used when `observedArea` is `None`
3. `None` geometry, Item still emitted as valid GeoJSON Feature with `"geometry": null`; WARNING logged

Spatial (Collection extent, computed per scope):
1. Union bbox of all Item bboxes within that scope's view of the Collection
2. If no Items in this scope have geometry: union of `Thing.Location` geometries directly
3. If neither: world bbox `[-180.0, -90.0, 180.0, 90.0]`; WARNING logged

Temporal (Item datetime):
1. `Datastream.phenomenonTime` parsed as ISO 8601 interval `start/end`
2. If `end` is `".."` or absent: `end_datetime = None`; `item["datetime"]` set to `start`
3. If both absent: Item is skipped entirely and a WARNING is logged, the only entity-level skip in the STAC transformer

Temporal (Collection extent, computed per scope):
1. `min(start_datetime)` across all successfully-built Items in that scope
2. `max(end_datetime)` across all Items in that scope; `None` if any Item is open-ended
3. If no Items were produced in this scope: `[[None, None]]` open interval

---

**Public interface:**
```python
def transform_to_stac(catalog: HarvestedCatalog) -> dict: ...
```
`transform_to_stac()` builds every scope's Catalog and Collection dicts directly, computing all `links`, `extent`, and Item entries per scope as described above, and returns one dict keyed by scope. Under `NETWORK=0` the return shape is `{"root": {"catalog": ..., "collections": {...}}}`, a single scope holding the full unscoped catalog, identical in content to the original pre-Network design. Under `NETWORK=1` the return shape is `{"root": {...orphan scope...}, "networks": {network_id: {"catalog": ..., "collections": {...}}, ...}}`. `cache.py` calls this function directly and writes each scope under its own key prefix; `api.py` reads from cache and serves the appropriate scope and sub-key per endpoint. Raises `ValueError` if `STAC_ROOT_HREF` is unset. Does not touch Postgres, Redis, or the STA HTTP API.