# STA to DCAT-AP 3.0 Mapping Reference

**Project:** istSOS Metadata Connector for Data Spaces and STAC
**Author:** Zala Vishmayraj
**Status:** Design, in implementation
**Scope:** `connector/dcat_transformer.py`, `connector/api.py` (DCAT endpoints)
**Input:** `HarvestedCatalog` from `connector/harvester.py` via cache
**Output:** DCAT-AP 3.0 Catalog, DatasetSeries, Dataset, and Distribution RDF graphs, served as `text/turtle`, compatible with data.europa.eu style harvesters
**Library:** `rdflib`. One independent `rdflib.Graph` per scope, no quad store, no named-graph dependency

---

## Design

This section is a self-contained overview of the DCAT-AP transformer. It is intended for quick review.

**The pivot decision.** One Datastream maps to one `dcat:Dataset`. One Thing maps to one `dcat:DatasetSeries`. The STA service root maps to `dcat:Catalog` and `dcat:DataService` (dual-typed on the same node).

The Datastream is the correct Dataset pivot for the same reason it is the correct STAC Item pivot: it is the only STA entity that simultaneously carries identity, description, spatial extent (`observedArea`), temporal extent (`phenomenonTime`), observation type, unit of measurement, and provenance via Sensor and ObservedProperty. The Thing is the correct DatasetSeries because it is the natural grouping unit, one physical device groups all its measurement channels, and `dcat:DatasetSeries` is the DCAT-AP 3.0 class meant for exactly this: a group of related Datasets. Series extents derive bottom-up from member Datasets, same semantics as the STAC Collection.

**What the transformer does:** it receives a `HarvestedCatalog` from the cache and builds DCAT-AP Catalog, DatasetSeries, and Dataset structures directly as triples in a plain `rdflib.Graph`, one independent Graph object per scope. All mandatory properties (`rdf:type`, `dct:title`, `dct:description`, `dct:identifier`) are asserted directly on the resource's `URIRef`. All relations (`dcat:hasPart` / `dct:isPartOf`, `dcat:distribution`, `dct:spatial`, `dct:temporal`) are constructed by hand using `_dataset_uri()` and `_thing_uri()`, which build absolute URIs from the same `HOSTNAME`, `SUBPATH`, and `VERSION` constants used by the STAC transformer. Named graphs (`rdflib.Dataset`) were considered and rejected in favor of independent per-scope `Graph` objects, see Network scoping below for why.

**Network scoping.** The connector reads the same `NETWORK` env var read by the STAC transformer, at the same harvest point. This determines which of the two harvest queries runs and whether scoping applies at all, identically to the STAC side:

- `NETWORK=0`: the original single-scope query runs, no `Network` join, no scoping logic. `/dcat/catalog.ttl` is the one and only catalog graph, exactly as in the pre-Network design.
- `NETWORK=1`: the extended query runs (see the harvester reference doc, this document does not own the SQL). The connector now serves multiple scopes side by side:
  - `/dcat/catalog.ttl` becomes the **root catalog**: a small, scope-invariant graph holding only the Catalog node, the DataService node, and Agent descriptions, plus `dct:hasPart` links out to each Network's own catalog graph and to the orphan catalog. It carries no Dataset or DatasetSeries content itself, the DCAT equivalent of root's `child` links in the STAC design.
  - `/dcat/{network_id}/catalog.ttl` **per Network**, an independent graph containing only DatasetSeries that have at least one Dataset in that Network, with each DatasetSeries's Dataset members filtered to that Network's Datastreams only.
  - `/dcat/orphan/catalog.ttl` is the **orphan scope**: Things that have at least one Datastream with `network_id IS NULL`, same grouping logic as a Network scope, just keyed on `network_id IS NULL` instead of a specific id, not a special case grafted on top.

`network_id` lives on `Datastream`, never on `Thing`, identical to the STAC design. A Thing has no scope of its own, a Thing's presence in a given scope is entirely a function of which of its Datastreams happen to fall into that scope (network). One consequence: a **Dataset never appears in more than one scope's graph**, since `network_id` is single-valued per row. Only **DatasetSeries** can appear in more than one scope, when a Thing's Datastreams are split across buckets, each scoped appearance is a distinct partial view of that Thing containing only the Datasets that fall into that scope.

**Why RDF needs a different scoping mechanism than STAC.** In STAC, a Collection's `id` and its serving URL are decoupled, so the same `id` can appear at two URLs with two different extents with no conflict. RDF does not have that split: a `URIRef` is the resource's identity, so if the same subject were asserted to have two different `dct:spatial` or `dct:temporal` values in one graph, a consumer just sees one resource with two conflicting values. This is confirmed as a real, non-theoretical case here, since one Thing's Datastreams can genuinely span multiple Networks, meaning its DatasetSeries needs a different computed extent per Network view.

Three approaches were evaluated:

1. **Named graphs (quad store).** Each Network gets its own labeled named graph inside an `rdflib.Dataset`. Structurally the textbook RDF answer, but pulls in a quad-store API, and Turtle cannot natively represent multiple named graphs (would require TriG), so every serialization step ends up flattening back to a plain `Graph` regardless. Rejected, more machinery than the problem requires.
2. **Flat graph with `dct:isPartOf` tagging.** One graph, one shared subject per Thing, disambiguated with a predicate stating which Network it belongs to. Breaks the moment a Thing's Datastreams span two Networks: the shared DatasetSeries subject ends up with conflicting multi-valued `dct:spatial` / `dct:temporal` triples with no way to tell which value applies to which view. Rejected, does not solve the actual problem.
3. **Independent per-scope graphs (selected).** One self-contained `rdflib.Graph` per Network, matching the root/networks split `transform_to_stac()` already returns for STAC. The same URI can carry different values across scopes because the two assertions never sit in the same graph object together. No quad store, no named-graph machinery, plain Turtle output at every scope.

DatasetSeries `dct:identifier` stays `thing-{id}` in every scope it appears in, it does not get a scope suffix. Uniqueness is enforced by which graph the triples live in, not by the identifier literal, the same principle STAC applies with href instead of id. Because each scoped DatasetSeries variant sees a different Dataset subset, its `dct:spatial` and `dct:temporal` must be computed per scope from that scope's own Datasets, not once globally and reused, the same physical Thing can legitimately have a different bbox and time interval depending on which scope's graph it is read from.

Consequence for `cache.py`, noted here since it originates from this design, actual key scheme is owned by the caching layer doc: the existing Redis key pattern needs to carry scope, `dcat:graph:{scope}:{id}`, mirroring the `stac:collection:{scope}:{id}` convention. `scope` is `net-{network_id}` for a Network view or a fixed sentinel (for example `orphan`) for the orphan view. Each scope's serialized Turtle is cached whole, there is no cross-scope merge to invalidate.

An optional aggregate "everything in one file" endpoint can be added later by unioning the independent per-scope graphs at serialize time only, never cached merged. 

**Known limitation:** if a Thing's Datastreams genuinely span two Networks, that single aggregate artifact will show the same DatasetSeries URI with two conflicting extents, since the isolation guarantee only holds within a single scope's graph. This mirrors a limitation the STAC design already defers rather than solves for its own flattened-view case.

**Why this isn't a defect in the current design, and why a unified extent is the wrong fix**. Within a single scope, the extent is always computed correctly and consistently, since it derives bottom-up only from that scope's own member Datasets, and the conflict above only arises in a hypothetical aggregate endpoint that unions multiple scopes' graphs together, not in any endpoint the design actually serves today. A tempting alternative would be computing one unified spatial/temporal extent per DatasetSeries and reusing it identically across every scope it appears in, which would make aggregation trivial. This was rejected: it would make every per-scope catalog internally inconsistent, a Network's own catalog would claim an extent broader than what its own dcat:hasPart list of Datasets justifies, on every single request, not just in some rare merged case. Worse, if Networks are intended as any kind of access or visibility boundary, a unified extent would leak the existence and rough location/timing of a Thing's Datastreams in other Networks to a consumer who should only be seeing this one, purely from the numbers not matching the listed members. Keeping extents computed strictly per scope trades a theoretical wrinkle in a feature that does not yet exist for a real, constant leak in a feature that does.

**Object hierarchy:**
```
NETWORK=0:
Graph                    (1, root, from STA service root)
  Catalog node            (1)
    DataService node       (1, dual-typed on Catalog node)
      DatasetSeries node    (1 per Thing, all Datastreams)
        Dataset node          (1 per Datastream)
          Distribution node     (3 per Dataset)

NETWORK=1:
Graph (root)              (1 -- Catalog, DataService, Agents, dct:hasPart links only)
Graph (per Network)        (1 per Network, independent)
  DatasetSeries node        (1 per Thing with >=1 Datastream in that Network)
    Dataset node               (1 per Datastream in that Network)
      Distribution node          (3 per Dataset)
Graph (orphan)              (1, independent)
  DatasetSeries node          (1 per Thing with >=1 orphan Datastream)
    Dataset node                  (1 per orphan Datastream)
      Distribution node              (3 per Dataset)
```

**STA entity role summary:**

| STA entity | DCAT-AP role | Representation | Notes |
|---|---|---|---|
| STA service root | `dcat:Catalog` + `dcat:DataService` | RDF node, dual-typed | Landing page graph. All mandatory identity properties from external config |
| Network | `dcat:Catalog` (sub-catalog) | RDF node, own graph | One sub-catalog graph per Network row, only when `NETWORK=1`. Linked from root via `dct:hasPart` |
| Thing | `dcat:DatasetSeries` | RDF node | One DatasetSeries per Thing per scope it appears in. Groups that scope's Datasets |
| Datastream | `dcat:Dataset` | RDF node | Pivot. One Dataset per Datastream, in exactly one scope |
| Location | `dct:spatial` extent + Dataset geometry fallback | `dct:Location` node with geometry literal | Primary spatial source is `Datastream.observedArea`. Location is fallback only |
| ObservedProperty | `dcat:keyword` + inline description | Literal | No dedicated node, values folded into keywords / description |
| Sensor | Dataset description fields | Literal | No dedicated node, values folded into `dct:description` |
| Observations collection | `dcat:Distribution` | RDF node | Constructed from Datastream URL pattern. No native STA entity |
| HistoricalLocation | not mapped | | Temporal data comes from `Datastream.phenomenonTime` |
| FeatureOfInterest | not mapped | | Per-observation spatial, not needed at catalog level |

**dcat:Catalog / dcat:DataService field mapping** (all identity fields from external config; `dct:title` for a Network sub-catalog uses the Network name)

Note: same harvester source as STAC, the connector reads Postgres directly via JOIN queries, there is no HTTP GET to the STA API. The exact query shape is owned by the harvester reference doc, not this one.

Mandatory:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `rdf:type` | Fixed | `dcat:Catalog, dcat:DataService` on the root node | Root node is dual-typed. Network sub-catalogs are `dcat:Catalog` only |
| `dct:identifier` | NONE | `config.DCAT_CATALOG_ID`, or the Network's own id for a sub-catalog | External config, one identifier per sub-catalog |
| `dct:title` | NONE | `config.DCAT_CATALOG_TITLE`, or Network name for a sub-catalog | External config with fallback |
| `dct:description` | Derived + external | `config.DCAT_CATALOG_DESCRIPTION or f"istSOS4 deployment: {thing_count} Things"` | Config with fallback to harvested Thing count |
| `dct:publisher` | NONE | `foaf:Agent` node from external config | See open question in Section 6 on whether sub-catalogs reuse the root Agent node |
| `dcat:hasPart` / `dct:hasPart` | Constructed manually | one triple per Network sub-catalog URI, only on the root graph | DCAT equivalent of root's `child` links in STAC |

Recommended:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `dcat:service` | Fixed | points to the `dcat:DataService` node | Only asserted on the root graph |
| `dct:conformsTo` | Fixed | DCAT-AP 3.0 profile URI | Hard-coded, not derived from STA conformance |
| `foaf:homepage` | Constructed URL | absolute URL of the catalog's own Turtle endpoint | One per Catalog node, root or sub-catalog |

**Catalog `dcat:dataset` flattening (deliberate).** Each scope's Catalog node asserts `dcat:dataset` twice over: once per member `DatasetSeries`, and once per individual `Dataset` belonging to any of those Series. This means `catalog dcat:dataset ?x` returns a mixed list of Series and Datasets rather than a clean single-level enumeration, a consumer has to check `rdf:type` to tell them apart.

This is intentional, not an oversight, and the reasoning is about who is expected to read this graph. Generic DCAT-AP harvesters, the "data.europa.eu style" consumers this document names as the target (see Scope, above), mostly walk `dcat:dataset` straight off a Catalog and have no notion of `dcat:DatasetSeries`, it is a newer, less universally-supported class. If the Catalog only pointed at Series and not at Datasets directly, a harvester like that would see a list of unfamiliar `DatasetSeries` nodes, have no path to walk into them, and never discover any of the underlying Datasets at all. Given that the explicit design goal is harvester compatibility, flattening is the safer choice: it costs a consumer that does understand the Series/Dataset distinction a slightly noisier top-level list, in exchange for a consumer that does not understand it still finding every Dataset.

The tradeoff would flip if a future consumer needed a clean two-level browse (Catalog to Series, Series to Dataset) more than it needed naive-harvester compatibility, but that is not the profile this connector is built for today.

**dcat:DatasetSeries field mapping** (one node per `HarvestedThing` per scope it has at least one Dataset in)

Mandatory:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `rdf:type` | Fixed | `dcat:DatasetSeries` | Always asserted |
| `dct:identifier` | `Thing.@iot.id` | `f"thing-{thing.id}"` | Same identifier used in every scope the Thing appears in. Uniqueness by which graph holds it, see Design section above |
| `dct:description` | `Thing.description` | `thing.description or f"DCAT DatasetSeries for SensorThings Thing: {thing.name}"` | Fallback composes Thing name into a minimal description |
| `dct:spatial` | Derived from this scope's Dataset geometries only | `dct:Location` node with a bbox-derived geometry literal | Computed per scope, not globally. World bbox fallback if none found in this scope |
| `dct:temporal` | Derived from this scope's Dataset `phenomenonTime` values only | `dct:PeriodOfTime` node with `dcat:startDate` / `dcat:endDate` | `dcat:endDate` absent for live / open-ended deployments |
| `dcat:hasPart` | Constructed manually | one triple per member Dataset in this scope | DCAT equivalent of STAC's `item` link |

Recommended:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `dct:title` | `Thing.name` | direct map | Omitted if `None` |
| `dcat:keyword` | `Thing.name` + union of `ObservedProperty.name` across this scope's Datastreams | one triple per keyword | `ObservedProperty.name` follows `category:subcategory:phenomenon_id` in dummy data, split on `:` and include each part |
| `dct:isPartOf` | Owning Network catalog for this scope | one triple pointing to the owning sub-catalog URI, points to the orphan catalog URI for the orphan scope | Not load-bearing for scope isolation, kept as a convenience for a consumer who has pulled one Dataset URI out of context |

Optional:

| DCAT-AP property | STA source | Notes |
|---|---|---|
| `dct:license` | `Thing.properties["license"]` if set, else `config.DCAT_DEFAULT_LICENSE` | Must resolve to a valid license URI, not the literal string `"other"` |
| `dct:accessRights` | NONE | External config, one per deployment |

**dcat:Dataset field mapping** (one node per Datastream, in exactly one scope)

Skip condition: same as STAC, if `Datastream.phenomenonTime` is absent or null and no `dct:temporal` value can be constructed, the Dataset is skipped entirely (WARNING logged). Null geometry is tolerated, null temporal with no fallback is not.

Mandatory:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `rdf:type` | Fixed | `dcat:Dataset` | Always asserted |
| `dct:identifier` | `Datastream.@iot.id` | `f"datastream-{ds['id']}"` | Prefixed for namespace clarity |
| `dct:title` | `Thing.name` + `Datastream.name` | `f"{thing.name} - {ds['name']}"` | Composed for uniqueness, same reasoning as STAC Item `title` |
| `dct:description` | `Datastream.description` | `ds["description"] or ""` | Empty string if absent, never omitted |
| `dct:spatial` | `Datastream.observedArea` | `dct:Location` node with geometry literal | Fallback: first `Thing.Location.location` |
| `dct:temporal` | `Datastream.phenomenonTime` | `dct:PeriodOfTime` node, `dcat:startDate` / `dcat:endDate` | `dcat:endDate` absent for open/live streams |
| `dcat:distribution` | Constructed | one triple per Distribution node, see below | Minimum 2 Distributions required per Dataset |

Recommended:

| DCAT-AP property | STA source | Construction | Notes |
|---|---|---|---|
| `dct:isPartOf` | Owning DatasetSeries in this scope | one triple pointing to the DatasetSeries URI | Round-trip navigation back to the parent Thing |
| `dcat:theme` / `dct:subject` | `ObservedProperty.name` | one triple per phenomenon segment | Same split-on-`:` logic as DatasetSeries keywords |
| `owl:sameAs` | `Datastream.@iot.id` self-link | points to the constructed STA URI `f"{base_url}/v1.1/Datastreams({ds['id']})"` | Round-trip navigation back to the STA entity, DCAT equivalent of STAC's `sta_datastream` link |

Optional:

| DCAT-AP property | STA source | Notes |
|---|---|---|
| `dct:conformsTo` | `Datastream.observationType` URI | e.g. `"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"` |
| Any extra property | `Datastream.properties[key]` | Pass through non-reserved keys as `dct:relation` or a custom predicate. Do not pass through `observedArea`, `phenomenonTime`, `@iot.*` |

**dcat:Distribution field mapping** (constructed from `ds["id"]`, Distribution URLs built the same way as STAC Asset hrefs)

| Distribution | `dcat:accessURL` | `dct:format` | `dcat:mediaType` | Consumer |
|---|---|---|---|---|
| Observations (JSON) | `{ds.self_link}/Observations` | `application/json` | `application/json` | Programmatic STA API consumers, real-time integrations |
| Observations (CSV) | `{ds.self_link}/Observations?$resultFormat=CSV` | `text/csv` | `text/csv` | Analytical pipelines, GIS tools, bulk export |
| Datastream metadata | `ds["self_link"]` | `application/json` | `application/json` | Round-trip navigation back to the STA Datastream entity |

Mandatory (per Distribution):

| DCAT-AP property | Source | Construction |
|---|---|---|
| `rdf:type` | Fixed | `dcat:Distribution` |
| `dcat:accessURL` | Constructed URL | must be an absolute URI |

Recommended (per Distribution):

| DCAT-AP property | Source | Notes |
|---|---|---|
| `dct:title` | `Datastream.name` + access-mode suffix | Disambiguation across the Dataset's Distributions |
| `dct:format` | Fixed per mode | IANA media type as a `dct:MediaTypeOrExtent` |
| `dct:description` | Fixed per mode | Short description of what the Distribution serves |

**Fallback chains:**

Spatial (Dataset `dct:spatial`):
1. `Datastream.observedArea`, preferred, per-variable spatial footprint
2. `Thing.Location.location`, first Location only, used when `observedArea` is `None`
3. `None`, Dataset still emitted with no `dct:spatial` triple; WARNING logged

Spatial (DatasetSeries `dct:spatial`, computed per scope):
1. Union bbox of all Dataset geometries within that scope's view of the DatasetSeries
2. If no Datasets in this scope have geometry: union of `Thing.Location` geometries directly
3. If neither: world bbox `[-180.0, -90.0, 180.0, 90.0]`; WARNING logged

Temporal (Dataset `dct:temporal`):
1. `Datastream.phenomenonTime` parsed as ISO 8601 interval `start/end`
2. If `end` is `".."` or absent: `dcat:endDate` omitted, `dcat:startDate` still set
3. If both absent: Dataset is skipped entirely and a WARNING is logged, the only entity-level skip in the DCAT transformer

Temporal (DatasetSeries `dct:temporal`, computed per scope):
1. `min(dcat:startDate)` across all successfully-built Datasets in that scope
2. `max(dcat:endDate)` across all Datasets in that scope; omitted if any Dataset is open-ended
3. If no Datasets were produced in this scope: no `dct:temporal` triple asserted

---

**Public interface:**
```python
def transform_to_dcat(catalog: HarvestedCatalog) -> dict[str, Graph | dict]: ...
```
`transform_to_dcat()` builds every scope's Catalog, DatasetSeries, and Dataset triples directly into independent `rdflib.Graph` objects, computing all relations, extents, and Dataset entries per scope as described above, and returns one dict keyed by scope. Under `NETWORK=0` the return shape is `{"root": Graph}`, a single scope holding the full unscoped catalog, identical in content to the original pre-Network design. Under `NETWORK=1` the return shape is `{"root": Graph (orphan + structural root), "networks": {network_id: Graph, ...}}`. `cache.py` calls this function directly and caches each scope's serialized Turtle under its own key prefix; `api.py` reads from cache and serves the appropriate scope's Turtle per endpoint. Raises `ValueError` if `DCAT_ROOT_HREF` is unset. Does not touch Postgres, Redis, or the STA HTTP API.

---