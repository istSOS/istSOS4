# STA to DCAT-AP 3.0 Mapping Reference

**Project:** istSOS Metadata Connector for Data Spaces and STAC
**Author:** Zala Vishmayraj
**Status:** Design, pre-implementation
**Scope:** `connector/dcat_transformer.py`, `connector/api.py` (DCAT endpoints)
**Input:** `HarvestedCatalog` from `connector/harvester.py` via cache
**Output:** DCAT-AP 3.0 RDF graph serialized as `application/ld+json`; Turtle available at `/dcat/catalog.ttl`
**Library:** `rdflib` >= 6.0

---

## Design

This section is a self-contained overview of the DCAT-AP transformer. It is intended for quick review.

**The pivot decision.** One Datastream maps to one `dcat:Dataset`. One Thing maps to one `dcat:DatasetSeries`. The STA service root maps to both `dcat:Catalog` and `dcat:DataService` (same URI, two `rdf:type` triples). Observations are not a native STA entity at catalog level; they are represented as constructed `dcat:Distribution` nodes.

The Datastream is the correct Dataset pivot because it is the only STA entity that simultaneously carries everything a DCAT-AP Dataset requires: identity, description, spatial extent (`observedArea`), temporal extent (`phenomenonTime`), observation type URI, unit of measurement, provenance via Sensor and ObservedProperty, and a stable URL for constructing Distributions. The Thing is the correct DatasetSeries because it is the natural grouping unit: one physical device groups all its measurement channels.

**What the transformer does:** It receives a `HarvestedCatalog` from the cache, builds a single `rdflib.Graph` containing all DCAT-AP statements, and returns the graph. The FastAPI layer serializes that graph to JSON-LD for `/dcat/catalog` or Turtle for `/dcat/catalog.ttl`. The transformer never touches the STA API directly. rdflib is used for all RDF graph construction because it validates namespace bindings, handles URI vs blank node construction, and serializes to multiple RDF formats from one graph object.

**Object hierarchy:**
```
dcat:Catalog       (1, root, same URI as DataService)
dcat:DataService   (1, same URI as Catalog)
  dcat:DatasetSeries (1 per Thing)
    dcat:Dataset     (1 per Datastream)
      dcat:Distribution (2-3 per Dataset)
```

**STA entity role summary:**

| STA entity | DCAT-AP role | RDF class | Notes |
|---|---|---|---|
| STA service root | `dcat:Catalog` | Named node (`{base_url}/`) | Groups all Datasets; deployment-level metadata |
| STA service root | `dcat:DataService` | Same named node as Catalog | Same URI, additional `rdf:type` triple |
| Thing | `dcat:DatasetSeries` | Named node (`{Thing.self_link}`) | Groups all Datastreams from the same device |
| Thing | `prov:Agent` | Same named node as DatasetSeries | Dual typing -- one URI, two `rdf:type` triples |
| **Datastream** | **`dcat:Dataset`** | **Named node (`{Datastream.self_link}`)** | **Pivot. One Dataset per Datastream.** |
| Location | `dct:Location` | Blank node (inline) | Contributes to `dct:spatial` on Dataset and DatasetSeries |
| Sensor | `foaf:Agent` (Creator) | Named node (`{Sensor.self_link}`) | Target of `dct:creator` on Dataset |
| Sensor | `prov:SoftwareAgent` | Same named node as Creator | Dual typing |
| ObservedProperty | `skos:Concept` | Named node (`{ObservedProperty.self_link}`) | Target of `dcat:theme` on Dataset |
| Observations collection | `dcat:Distribution` | Named node (constructed URL) | 2-3 per Datastream; no native STA entity; constructed from Datastream URL pattern |
| HistoricalLocation | not mapped | | Temporal data comes from `Datastream.phenomenonTime` |
| FeatureOfInterest | not mapped | | Per-observation spatial, not needed at catalog metadata level |

---

**dcat:Catalog field mapping** (all identity fields from external config; counts from `len(catalog.things)` and sum of `len(t.datastreams)` across things)

Note: the harvester reads Postgres directly via a single JOIN query. There is no HTTP GET to the STA API. All STA entity data arrives as fields on `HarvestedThing` / `HarvestedCatalog` dataclass instances. STA-style URIs (`/v1.1/Things({id})` etc.) used as RDF subjects are constructed by the transformer from `base_url` + entity `id`; they are not stored in the harvested data.

All three mandatory fields have no STA source and require external configuration. The connector will not produce a spec-compliant `dcat:Catalog` without at minimum `DCAT_CATALOG_TITLE`, `DCAT_CATALOG_DESCRIPTION`, and `DCAT_PUBLISHER_NAME` / `DCAT_PUBLISHER_URI` set in the environment.

Mandatory:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dct:title` | NONE | `Literal(config.DCAT_CATALOG_TITLE, lang="en")` | External config. Hard-warn on startup if unset |
| `dct:description` | Derived + external | `Literal(f"{config.DCAT_CATALOG_DESCRIPTION}. Contains {thing_count} Things...", lang="en")` | Compose from config + harvested Thing and Datastream counts |
| `dct:publisher` | NONE | `URIRef(config.DCAT_PUBLISHER_URI)` | External config. Shared Agent node across all Catalog, Dataset, and DataService records |

Recommended:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dcat:dataset` | All Datastreams | `g.add((catalog_uri, DCAT.dataset, dataset_uri))` per Datastream | One triple per Dataset |
| `dcat:service` | STA endpoint | `g.add((catalog_uri, DCAT.service, service_uri))` | Links to the `dcat:DataService` record |
| `dct:license` | NONE | `URIRef(config.DCAT_DEFAULT_LICENSE)` | Catalog-level default. Datastream-level `properties["license"]` overrides |
| `dct:issued` | Min `phenomenonTime` start across all Datastreams | `Literal(min_start, datatype=XSD.dateTime)` | Derived by scanning all `phenomenon_time` fields |
| `dct:modified` | Max `phenomenonTime` end across all Datastreams | `Literal(max_end, datatype=XSD.dateTime)` | Omitted if any Datastream is live (open-ended) |
| `dct:language` | Inferred | `URIRef("http://id.loc.gov/vocabulary/iso639-1/en")` | Defaults to English; configurable |
| `foaf:homepage` | `base_url` | `URIRef(base_url)` | Points to the STA service root as the human-accessible entry point |
| `dct:spatial` | Union of `Datastream.observedArea` bboxes | `dct:Location` blank node | Union bbox across all Datastreams and Locations. See Spatial and Temporal Computation |
| `dct:temporal` | Min start / max end across all Datastreams | `dct:PeriodOfTime` blank node | See Spatial and Temporal Computation |

Optional (selected):

| DCAT-AP property | STA source | Notes |
|---|---|---|
| `dcat:record` | Per Datastream | `CatalogRecord` blank node with `@iot.id` as `dct:identifier`. No native audit trail in STA |
| `dct:hasPart` / `dct:isPartOf` | NONE | Federation relationships for multi-STA-endpoint networks |
| `dcat:applicableLegislation` | NONE | ELI URIs from config. Mandatory for HVD compliance |

---

**dcat:Dataset field mapping** (`thing.datastreams[]` -- one Dataset per Datastream dict in `HarvestedThing`)

Mandatory:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dct:title` | `Datastream.name` + `Thing.name` | `Literal(f"{thing.name} — {ds['name']}", lang="en")` | Compound title for uniqueness. Datastream names alone are often not unique within a deployment |
| `dct:description` | `Datastream.description` | `Literal(_compose_description(ds, thing), lang="en")` | Primary from Datastream description; fallback supplements from `ObservedProperty.description` and `Sensor.description` if empty |

Recommended:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dct:publisher` | NONE | `URIRef(config.DCAT_PUBLISHER_URI)` | Same shared Agent node as Catalog. Hard-warn on startup if unset |
| `dcat:keyword` | `ObservedProperty.name` + `Thing.name` | `Literal` per keyword | `ObservedProperty.name` in the dummy data follows `category:subcategory:phenomenon_id`; split on `:` and emit each part separately. Supplement with `Thing.name` and `Datastream.properties["keywords"]` |
| `dcat:theme` | `ObservedProperty.definition` (URI) | `URIRef(op["definition"])` if valid URI | If `definition` is a valid URI (QUDT, INSPIRE, CF Standard Names), emit directly. If not (e.g. `"{}"` in dummy data), fall back to `Datastream.properties["theme"]`. Map to EU Data Theme URI if a lookup match exists |
| `dct:spatial` | `Datastream.observedArea` | `dct:Location` blank node with `geosparql:asGeoJSON` literal | Fallback: union bbox from `Thing.locations[*].geometry`. See Spatial and Temporal Computation |
| `dct:temporal` | `Datastream.phenomenonTime` | `dct:PeriodOfTime` blank node with `dcat:startDate` / `dcat:endDate` | Parsed from `"start/end"` interval string. `dcat:endDate` omitted for live streams. Both typed as `xsd:dateTime` |
| `dct:accessRights` | NONE | `URIRef(eu_mdr_access_right)` | From `Datastream.properties["accessRights"]`. EU MDR vocab: `PUBLIC`, `RESTRICTED`, `NON_PUBLIC` |

Optional (selected):

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dct:identifier` | `Datastream.@iot.id` | `URIRef(ds["self_link"])` | STA selfLink is the stable identifier. Also the RDF subject URI |
| `dct:issued` | `phenomenonTime` start | `Literal(start, datatype=XSD.dateTime)` | Fallback when Observation `resultTime` is not harvested |
| `dct:modified` | `phenomenonTime` end | `Literal(end, datatype=XSD.dateTime)` | Omitted for live streams |
| `dct:conformsTo` | `Datastream.observationType` URI | `URIRef(ds["observation_type"])` | Also add OGC STA 1.1 URI: `http://www.opengis.net/spec/iot_sensing/1.1` |
| `dct:accrualPeriodicity` | `properties["accrualPeriodicity"]` | `URIRef(eu_mdr_freq_uri)` | EU MDR frequency vocabulary |
| `dct:provenance` | `Sensor.metadata` | `Literal(sensor["metadata"])` | Calibration records, SensorML references, instrument datasheets |
| `dct:creator` | `Sensor` entity | `URIRef(sensor["self_link"])` | Same URI as the `foaf:Agent` (Creator) node for this Sensor. See Agent Modeling |
| `dcat:landingPage` | `Datastream.self_link` | `URIRef(ds["self_link"])` | The STA endpoint page for this Datastream |
| `dcat:inSeries` | Parent `Thing` | `URIRef(thing.self_link)` | Links Dataset to its parent DatasetSeries |
| `prov:wasGeneratedBy` | Parent `Thing` | `URIRef(thing.self_link)` | The Thing (sensor platform) generated this Datastream's observations. Same URI as the `prov:Agent` node for the Thing |
| `dct:isReferencedBy` | NONE | `URIRef` per entry | From `Datastream.properties["isReferencedBy"]` array. DOI URIs for associated publications |

---

**dcat:Distribution field mapping** (constructed from `Datastream.self_link` -- 2-3 Distributions per Dataset)

STA has no Distribution entity. Distributions are synthesized by the transformer at mapping time. Three are created per Datastream covering the three materially different access modes. The MQTT Distribution is only emitted if `MQTT_BROKER_URL` is set in config.

| Access mode | `dcat:accessURL` | `dcat:mediaType` | Consumer |
|---|---|---|---|
| REST / JSON | `{ds.self_link}/Observations` | `https://www.iana.org/assignments/media-types/application/json` | Programmatic API consumers |
| CSV bulk | `{ds.self_link}/Observations?$resultFormat=CSV` | `https://www.iana.org/assignments/media-types/text/csv` | Analytical pipelines, GIS tools |
| MQTT stream | `mqtt://{config.MQTT_BROKER_URL}/v1.1/Datastreams({id})/Observations` | `application/json` | Real-time subscribers |

Mandatory (per Distribution):

| DCAT-AP property | Source | RDF construction |
|---|---|---|
| `dcat:accessURL` | Constructed URL | `URIRef(constructed_url)` |

Recommended (per Distribution):

| DCAT-AP property | Source | RDF construction | Notes |
|---|---|---|---|
| `dcat:mediaType` | Fixed per mode | `URIRef(iana_media_type_uri)` | Use full IANA URI, not bare string |
| `dct:format` | Fixed per mode | `URIRef(eu_mdr_filetype_uri)` | EU MDR file-type vocabulary. JSON: `.../JSON`. CSV: `.../CSV` |
| `dcat:downloadURL` | CSV mode only | `URIRef(csv_url_with_top)` | Omit for REST and MQTT (streaming, not downloadable) |
| `dct:license` | `Datastream.properties["license"]` | `URIRef(license_uri)` | Inherit from Catalog-level default if absent on Datastream |
| `adms:status` | Derived from `phenomenonTime` end | `URIRef(adms_status_uri)` | `null` or absent end → `adms:UnderDevelopment` (live). Past end → `adms:Completed` |
| `dct:title` | `Datastream.name` + mode suffix | `Literal(f"{ds['name']} — CSV export", lang="en")` | Composed per mode |
| `dcat:accessService` | STA endpoint | `URIRef(service_uri)` | Links back to the `dcat:DataService`. One triple per Distribution |

---

**dcat:DataService field mapping** (one record for the entire STA endpoint, same URI as Catalog)

One DataService record is emitted for the entire STA endpoint. It shares its URI with the `dcat:Catalog` node. In RDF, the same subject carrying both `rdf:type dcat:Catalog` and `rdf:type dcat:DataService` is valid and correct: the STA endpoint is simultaneously a catalog of datasets and a service that provides access to them.

Mandatory:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dcat:endpointURL` | `base_url` | `URIRef(base_url + "/")` | The STA service root. Trailing slash |
| `dct:title` | NONE | `Literal(config.DCAT_SERVICE_TITLE, lang="en")` | External config. Pattern: `"[Network name] IoT Sensor Service"` |
| `dcat:servesDataset` | All Datastreams | `g.add((service_uri, DCAT.servesDataset, ds_uri))` per Datastream | One triple per harvested Datastream |

Recommended:

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `dcat:endpointDescription` | STA OpenAPI + `/Conformance` | Two `URIRef` values | `{base_url}/api` for OpenAPI. `{base_url}/Conformance` if available; otherwise read from `HarvestedCatalog.conformance` |
| `dct:conformsTo` | OGC STA standard URI | `URIRef("http://www.opengis.net/spec/iot_sensing/1.1")` | Hard-code OGC STA 1.1 URI. Also add individual conformance URIs from `HarvestedCatalog.conformance` |
| `dct:publisher` | NONE | `URIRef(config.DCAT_PUBLISHER_URI)` | Same Agent node as Catalog and all Datasets |
| `dcat:keyword` | Union of `Sensor.name` + `ObservedProperty.name` | `Literal` per keyword | Deduplicated union across all Things in catalog |
| `dcat:theme` | Union of `ObservedProperty.definition` URIs | `URIRef` per valid URI | Same EU Data Theme lookup as Dataset-level `dcat:theme` |

Optional (selected):

| DCAT-AP property | STA source | Notes |
|---|---|---|
| `dct:issued` | Min `phenomenonTime` start across all Datastreams | `Literal(min_start, datatype=XSD.dateTime)` |
| `dct:modified` | Max `phenomenonTime` end across all Datastreams | `Literal(max_end, datatype=XSD.dateTime)` |
| `dct:spatial` | Union of all `Datastream.observedArea` bboxes | `dct:Location` blank node. See Spatial and Temporal Computation |
| `dct:temporal` | Min start / max end across all Datastreams | `dct:PeriodOfTime` blank node. See Spatial and Temporal Computation |
| `foaf:homepage` | `base_url` | `URIRef(base_url)` |

---

**dcat:DatasetSeries field mapping** (`GET {STA_BASE_URL}/Things` -- one DatasetSeries per Thing)

Each Thing becomes a DatasetSeries node. The same URI is also typed as `prov:Agent`. The primary purpose of this node is to act as the target of `dcat:inSeries` triples from each of the Thing's Datasets, making the device→measurement-channel hierarchy queryable in the RDF graph.

| DCAT-AP property | STA source | RDF construction | Notes |
|---|---|---|---|
| `rdf:type` | Fixed | `DCAT.DatasetSeries` | Also `PROV.Agent` on same node |
| `dct:title` | `Thing.name` | `Literal(thing.name, lang="en")` | |
| `dct:description` | `Thing.description` | `Literal(thing.description, lang="en")` | Omitted if absent |
| `dct:identifier` | `Thing.self_link` | `URIRef(thing.self_link)` | |
| `dct:spatial` | `Thing.locations[*].geometry` | `dct:Location` blank node | Spatial extent of the sensor platform, derived from Location geometries |
| `foaf:homepage` | `Thing.self_link` | `URIRef(thing.self_link)` | |

---

**Agent modeling** (foaf:Agent roles)

The connector models four agent roles from the STA data.

| Role | RDF types | URI pattern | `foaf:name` source | Notes |
|---|---|---|---|---|
| Publisher | `foaf:Agent`, `org:Organization` | `{DCAT_PUBLISHER_URI}` (external) | `config.DCAT_PUBLISHER_NAME` | Shared node. Referenced by Catalog, all Datasets, DataService |
| Creator (Sensor) | `foaf:Agent`, `prov:SoftwareAgent` | `{Sensor.self_link}` | `Sensor.name` | `Sensor.metadata` emitted as `foaf:homepage` if it is a valid URL |
| Thing-as-Agent | `dcat:DatasetSeries`, `prov:Agent` | `{Thing.self_link}` | (inherited from DatasetSeries `dct:title`) | Same URI as DatasetSeries. Target of `prov:wasGeneratedBy` on each Dataset |
| Contact | `vcard:Kind` | BNode (no stable URI) | `Thing.properties["contactPoint"]["fn"]` | Constructed from `Thing.properties["contactPoint"]` if present |

---

**Spatial and temporal computation**

The harvester stores raw spatial and temporal data without computing derived values. All computation happens in the transformer.

Temporal (Dataset):
1. `Datastream.phenomenonTime` is a string in `"start/end"` format. Split on `/` to get start and end.
2. If `end` is `".."` or absent: `dcat:endDate` is omitted. Live stream.
3. Both start and end typed as `xsd:dateTime` literals.

Temporal (Catalog / DataService):
1. `min(all start values)` across all Datastreams → catalog `dcat:startDate`
2. `max(all end values)` across all Datastreams → catalog `dcat:endDate`
3. If any Datastream is live (no end), catalog-level `dcat:endDate` is omitted

Spatial (Dataset geometry):
1. `Datastream.observedArea` -- preferred; per-variable spatial footprint
2. Union bbox from parent `Thing.locations[*].geometry` -- fallback if `observedArea` is `None`
3. Omit `dct:spatial` triple if neither is available; WARNING logged

Spatial (DatasetSeries extent):
1. Union of all `Thing.Location.geometry` values on the Thing
2. For Point geometries (typical station deployments): bounding box from all Point coordinates

Spatial (Catalog / DataService extent):
1. Union bbox across all Datastream `observedArea` polygons and all Location geometries
2. Emitted as WKT literal typed `geosparql:asWKT` for compatibility with CKAN and EU Data Portal harvesters

---

**Gap handling: properties convention**

Fields with no STA source can be supplied by deployment operators via the `properties` dict on Datastream or Thing entities. The transformer reads these keys when present. Operators who do not set them get the behavior described in the Notes column.

| DCAT-AP property | `properties` key | Expected value | Entity | Default behavior if absent |
|---|---|---|---|---|
| `dct:license` | `license` | CC license URI, e.g. `"https://creativecommons.org/licenses/by/4.0/"` | Thing, Datastream | Inherit `config.DCAT_DEFAULT_LICENSE` |
| `dct:rights` | `rights` | EU MDR access-right URI | Thing, Datastream | Triple omitted |
| `dct:accessRights` | `accessRights` | EU MDR: `PUBLIC` / `RESTRICTED` / `NON_PUBLIC` URI | Datastream | Triple omitted |
| `dct:accrualPeriodicity` | `accrualPeriodicity` | EU MDR frequency URI | Datastream | Triple omitted |
| `dcat:applicableLegislation` | `applicableLegislation` | `["http://data.europa.eu/eli/..."]` | Datastream | Triple omitted |
| `dct:version` | `version` | `"2.1"` | Datastream | Triple omitted |
| `adms:versionNotes` | `versionNotes` | Free text | Datastream | Triple omitted |
| `dcat:contactPoint` | `contactPoint` | `{"fn": "IoT Team", "email": "mailto:ops@example.org"}` | Thing | Triple omitted |
| `dct:isReferencedBy` | `isReferencedBy` | `["https://doi.org/10.5194/..."]` | Datastream | Triple omitted |
| `dcat:spatialResolutionInMeters` | `spatialResolutionInMeters` | `10.5` (float) | Datastream, Sensor | Triple omitted |
| `odrl:hasPolicy` | `odrlPolicy` | Policy URI | Datastream | Triple omitted |
| `dcat:theme` (override) | `theme` | EU Data Theme URI | Datastream | Triple omitted when `ObservedProperty.definition` is not a valid URI |
| `dcat:keyword` (extra) | `keywords` | `["rainfall", "hydrology"]` | Datastream | No extra keywords beyond those derived from `ObservedProperty.name` |

External config fields (environment variables) required for spec-compliant output:

| Env variable | Affects | Required |
|---|---|---|
| `DCAT_PUBLISHER_NAME` | Catalog, all Datasets, DataService | Yes |
| `DCAT_PUBLISHER_URI` | Catalog, all Datasets, DataService | Yes |
| `DCAT_CATALOG_TITLE` | Catalog | Yes |
| `DCAT_CATALOG_DESCRIPTION` | Catalog | Yes |
| `DCAT_SERVICE_TITLE` | DataService | No (default: `"istSOS4 SensorThings API"`) |
| `DCAT_DEFAULT_LICENSE` | Catalog, Distributions | No |
| `DCAT_ACCESS_RIGHTS` | Catalog, DataService | No |
| `MQTT_BROKER_URL` | MQTT Distribution | No (MQTT Distribution omitted if unset) |

---

**Public interface:**
```python
def build_dcat_graph(catalog: HarvestedCatalog, config: Settings) -> Graph: ...
def serialize_dcat(graph: Graph, format: str = "json-ld") -> str: ...
```
`build_dcat_graph()` constructs the full rdflib Graph, adds all DCAT-AP triples, and returns the graph object. Raises `DCATTransformerConfigurationError` if mandatory config fields are missing and `config.DCAT_STRICT_MODE` is `True`; otherwise logs warnings and returns a parseable-but-invalid graph. `serialize_dcat()` serializes the graph to the requested format (`"json-ld"`, `"turtle"`, or `"n-triples"`). Returns a string. Neither function touches the cache or HTTP layer.