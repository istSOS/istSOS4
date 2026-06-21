# OGC SensorThings API v1.1 — Coverage Matrix (istSOS4)

Authoritative cross-reference between what the service **declares** in
`serverSettings.conformance` and the conformance suite that proves it. Section 0
is the per-URI ledger (the contract: every declared URI → a passing test).
Sections 1+ keep the A/B/C/GAP analysis used to build the suite.

| Set | Meaning | Source |
|---|---|---|
| **Decl** | Declared in the live `serverSettings.conformance` | live root `GET /v4/v1.1` |
| **A** | Exercised by the OGC TeamEngine compliance run | `tests/docs/ENGINE_REQUESTS.txt` (2387 blocks → 1495 unique) |
| **B** | Present in FROST-Server **v2.7.2** c01/c02/c03 tests (extensions excluded) | `~/workspace/GIT/FROST-Server` (read-only, tag v2.7.2) |
| **C** | Required/defined by OGC 18-088 | §8/§9/§10, esp. §9.3.3 + §9.3.3.5.2 Table 23 |

> **Adjudication rule (updated):** `serverSettings.conformance` was **expanded** and
> now declares `built-in-query-functions`, `count`, `pagination`, `status-code`,
> `query-status-code`, `update-entity-put`, `historical-location-auto-creation` and
> `data-array/data-array`. **Declaring a class is a promise:** every URI must have a
> test that **passes**. A deviation on a **declared** URI is a real 18-088 violation →
> routed to api-fixer (never `xfail`). A 500 is never acceptable. Items in **B but not
> C** (FROST extensions) remain **out-of-scope** — listed in §13, not implemented.
> There are **no `xfail`s**: `contains` — an OData-4.01 alias **not** in 18-088 Table 23
> (the spec's substring predicate is `substringof`, which is implemented and tested) — is
> **out-of-scope** and no longer tested (see §14).

**Suite result:** `405 passed, 0 xfailed, 0 failed` (`-n auto`).
Per class: c01 **203**, c02 **73**, c03 **120**, data_array **9**.
(Baseline before this round: 350 passed / 5 xfailed.)

Legend: ✔ yes · — no · ~ partial.

---

## 0. Declared-URI conformance ledger

Every URI in the live `serverSettings.conformance`, its status, and the test(s)
that cover it. **Status: covered** unless noted. Nothing declared is MISSING or in
VIOLATION.

### Data model (16 + control info)

| Declared URI (`…/req/…`) | Status | Covering test(s) — file |
|---|---|---|
| `datamodel/thing/properties` | covered | `test_thing_mandatory_properties` — c01 |
| `datamodel/thing/relations` | covered | `test_thing_relations` — c01 |
| `datamodel/location/properties` | covered | `test_location_mandatory_properties` — c01 |
| `datamodel/location/relations` | covered | `test_location_relations` — c01 |
| `datamodel/historical-location/properties` | covered | `test_historical_location_mandatory_properties` — c01 |
| `datamodel/historical-location/relations` | covered | `test_historical_location_relations` — c01 |
| `datamodel/datastream/properties` | covered | `test_datastream_mandatory_properties_ds1` / `_ds2` — c01 |
| `datamodel/datastream/relations` | covered | `test_datastream_relations` — c01 |
| `datamodel/sensor/properties` | covered | `test_sensor_mandatory_properties` — c01 |
| `datamodel/sensor/relations` | covered | `test_sensor_relations` — c01 |
| `datamodel/observed-property/properties` | covered | `test_observed_property_mandatory_properties` — c01 |
| `datamodel/observed-property/relations` | covered | `test_observed_property_relations` — c01 |
| `datamodel/observation/properties` | covered | `test_observation_mandatory_properties` — c01 |
| `datamodel/observation/relations` | covered | `test_observation_relations` — c01 |
| `datamodel/feature-of-interest/properties` | covered | `test_foi_mandatory_properties` — c01 |
| `datamodel/feature-of-interest/relations` | covered | `test_foi_relations` — c01 |
| `datamodel/entity-control-information/common-control-information` | covered | `test_*_has_iot_id_and_self_link`, `test_*_navigation_links`, `test_collection_items_have_control_info`, `test_self_link_resolves_to_same_entity` — c01 |

### Resource path

| Declared URI | Status | Covering test(s) |
|---|---|---|
| `resource-path/resource-path-to-entities` | covered | `test_*_by_id`, `test_historical_location_by_id`, `test_*_to_*` navigation + `$ref` tests — c01 |

### Request data / query options

| Declared URI | Status | Covering test(s) |
|---|---|---|
| `request-data/order` | covered | `test_orderby_*`, `test_combination_all_options` — c03 (alias of orderby) |
| `request-data/orderby` | covered | `test_orderby_result_asc/desc`, `test_orderby_phenomenontime_asc/desc`, `test_orderby_multikey_*`, `test_orderby_default_is_ascending` — c03 |
| `request-data/top` | covered | `test_top_limits_results`, `test_top_skip_windows_no_overlap` — c03; `test_top_limits_result_count` — c01 |
| `request-data/skip` | covered | `test_skip_offsets_results`, `test_skip_beyond_end_is_empty`, `test_top_skip_windows_no_overlap` — c03 |
| `request-data/select` | covered | `test_select_single_property`, `test_select_multiple_properties`, `test_select_navigation_property` — c03 |
| `request-data/expand` | covered | `test_expand_single/multiple/nested_path/nested_with_options/nested_filter_on_expanded_set` — c03 |
| `request-data/filter` | covered | `test_relation_*`, `test_edge_*`, `test_seed_scope_*` — c03; datetime/geo predicate tests |
| `request-data/built-in-filter-operations` | covered | `test_eq/ne/gt/ge/lt/le/and/or/not/precedence_parentheses/add/sub/mul/div/mod` — c03 (logic_arith) |
| `request-data/built-in-query-functions` | covered | string (`startswith`…`concat`, **`substringof`**), datetime (`year`…`maxdatetime`), geo (`geo.*`, `st_*`, **`geo.length` literal**), math (`round/floor/ceiling`) — c03 |
| `request-data/count` | covered | `test_count_*` incl. **`test_count_empty_set_returns_zero`** — c03; `test_count_*` — c01 |
| `request-data/pagination` | covered | **`test_pagination_top_nextlink_walks_full_seed`**, `test_nextlink_paging_no_overlap_no_gaps`, `test_nextlink_present_when_results_exceed_page`, `test_nextlink_absent_when_all_fit`, `test_nextlink_per_datastream_walk` — c03; `test_next_link_*` — c01 |
| `request-data/status-code` | covered | `test_service_root_returns_200`, `test_collection_get_returns_200`, `test_entity_by_id_returns_200`, `test_navigation_link_returns_200`, `test_many_to_one_nav_returns_200` — c01 |
| `request-data/query-status-code` | covered | `test_bad_filter_syntax_returns_400`, `test_orderby_unknown_property_returns_400`, `test_negative_top_returns_400`, `test_negative_skip_returns_400`, `test_filter_unknown_function_returns_400`, `test_400_body_is_not_stacktrace` — c01 |

### Create / Update / Delete

| Declared URI | Status | Covering test(s) |
|---|---|---|
| `create-update-delete/create-entity` | covered | `test_post_*` (8 collections), `test_post_to_navigation_link_*`, `test_validation_*` — c02 |
| `create-update-delete/link-to-existing-entities` | covered | `test_create_with_existing_link`, `test_create_observation_with_existing_foi_link` — c02 |
| `create-update-delete/deep-insert` | covered | `test_deep_insert` — c02 (+ `seed` fixture) |
| `create-update-delete/deep-insert-status-code` | covered | `test_deep_insert` (asserts 201 + Location) — c02 |
| `create-update-delete/update-entity` | covered | `test_patch_*` (10, incl. partial + relink) — c02 |
| `create-update-delete/delete-entity` | covered | `test_delete_*` (8) + `test_cascade_delete_*` + `test_delete_nonexistent_*` — c02 |
| `create-update-delete/historical-location-auto-creation` | covered | **`test_historical_location_auto_creation`** — c02 |
| `create-update-delete/update-entity-put` | covered | **`test_put_replace_thing`**, **`test_put_missing_mandatory_property_thing`**, **`test_put_missing_mandatory_property_sensor`**, **`test_put_optional_property_reset`** — c02 |
| `create-update-delete/update-entity-jsonpatch` | covered | `test_jsonpatch_*` (add/replace/copy/move/remove/test on Thing+Datastream) — c02 (jsonpatch) |

### Data Array extension

| Declared URI | Status | Covering test(s) |
|---|---|---|
| `data-array/data-array` | covered | **`test_data_array_read_structure`**, `test_data_array_read_values_ds1`/`_ds2`, `test_data_array_read_top`, `test_data_array_read_orderby`, `test_data_array_collection_path`, `test_create_observations_data_array`, `test_create_observations_missing_result_component`, `test_create_observations_missing_datastream` — data_array |

**Bold** = added/converted this round. Nothing declared is uncovered.

---

## 1. System query options

| Item | Decl | A | B | C | Cover |
|---|:--:|:--:|:--:|:--:|---|
| Service root `value[]` + `serverSettings.conformance` | ✔ | ✔ | ✔ | ✔ | c01 |
| Collection GET (8 sets) | ✔ | ✔ | ✔ | ✔ | c01 |
| Entity by id | ✔ | ✔ | ✔ | ✔ | c01 |
| Property access `/prop` + `/prop/$value` (raw text/plain) | ✔ | ✔ | ✔ | ✔ | c01 (api-fixed earlier) |
| `$ref` (collection + singleton) | ✔ | ✔ | ✔ | ✔ | c01 |
| `$orderby` single/multiple | ✔ | ✔ | ✔ | ✔ | c03 |
| `$top` / `$skip` paging | ✔ | ✔ | ✔ | ✔ | c03 |
| `@iot.nextLink` full traversal (no overlap/gaps) | ✔ | ✔ | ✔ | ✔ | c03 `test_pagination_top_nextlink_walks_full_seed` |
| `$count=true` (non-empty / with top / per-ds) | ✔ | ✔ | ✔ | ✔ | c03 |
| `$count=false` omits annotation | ✔ | — | ✔ | ✔ | c03 |
| **`$count=true` empty set → `@iot.count:0`** | ✔ | — | — | ✔ | **c03 `test_count_empty_set_returns_zero` (api-fixed [B], was xfail)** |
| `$select` props / multiple / **navigation** | ✔ | ~ | ✔ | ✔ | c03 |
| `$expand` single / multiple / nested-path / **nested-with-options** | ✔ | ~ | ✔ | ✔ | c03 |

## 2–4. `$filter` comparison / logical / arithmetic (Decl=✔ built-in-filter-operations)

| Operators | Decl | Cover |
|---|:--:|---|
| `eq ne gt ge lt le` | ✔ | c03 logic_arith |
| `and or not` + parenthesised precedence | ✔ | c03 (`not` api-fixed earlier) |
| `add sub mul div mod` | ✔ | c03 |

## 5. `$filter` string functions (Decl=✔ built-in-query-functions)

| Function | Decl | Cover |
|---|:--:|---|
| `startswith endswith length indexof substring(1&2-arg) tolower toupper trim concat` | ✔ | c03 — all pass |
| **`substringof`** | ✔ | **c03 `test_substringof` (api-fixed [A]: was `[]`, now LIKE; was xfail)** |
| `contains` | — | **out-of-scope** — OData-4.01 alias, NOT in 18-088 Table 23 (istSOS4 → 400). Not a required fn of any declared class; no longer tested (see §14). |

## 6–7. `$filter` datetime / math functions (Decl=✔ built-in-query-functions)

| Functions | Decl | Cover |
|---|:--:|---|
| `year month day hour minute second date fractionalseconds totaloffsetminutes now mindatetime maxdatetime time` | ✔ | c03 datetime |
| `round floor ceiling` | ✔ | c03 logic_arith |

## 8–9. `$filter` geospatial / spatial-relationship (Decl=✔ built-in-query-functions)

| Functions | Decl | Cover |
|---|:--:|---|
| `geo.distance geo.length geo.intersects` | ✔ | c03 geo |
| **`geo.length(geography'LINESTRING(...)')` (literal)** | ✔ | **c03 `test_geo_length_literal_linestring` (api-fixed [C]: was 400; was xfail)** |
| `st_equals st_disjoint st_touches st_within st_overlaps st_crosses st_intersects st_contains st_relate` | ✔ | c03 geo |

## 10. `$filter` across related entities (Decl=✔ filter)

| Form | Decl | Cover |
|---|:--:|---|
| 1-hop / 2-hop / deep multi-hop / by-id | ✔ | c03 logic_arith `test_relation_*` |
| combinations ($filter+$orderby+$top+$select+$expand) | ✔ | c03 `test_combination_*` |

## 11. Create / Update / Delete (Decl=✔)

| Operation | Decl | Cover |
|---|:--:|---|
| POST minimal each collection (201 + Location) | ✔ | c02 |
| Deep insert (+ status-code) | ✔ | c02 |
| Link-to-existing via `{"@iot.id"}` | ✔ | c02 |
| **POST-to-navigation-link create (Req 33): `Things(id)/Datastreams`** | ✔ | **c02 `test_post_to_navigation_link_thing_datastreams`** |
| **`Sensors(id)/Datastreams` / `ObservedProperties(id)/Datastreams`** | ✔ | **c02 `..._sensor_datastreams` / `..._observedproperty_datastreams`** |
| **`Locations(id)/Things` / `Things(id)/HistoricalLocations`** (edge) | ✔ | **c02 `..._location_things` / `..._thing_historicallocations`** |
| **`FeaturesOfInterest(id)/Observations`** (fix 500→201, §15 E) | ✔ | **c02 `..._foi_observations`** |
| `Things(id)/Locations` / `Datastreams(id)/Observations` (pre-existing) | ✔ | c02 `..._thing_locations` / `..._datastream_observations` |
| FeatureOfInterest auto-generation | ✔ | c02 |
| PATCH (partial merge) + link change | ✔ | c02 |
| **PUT (full replace)** | ✔ | **c02 `test_put_*` (api-fixed [D]: was 405; was xfail). Missing mandatory → 400; omitted optional → null; id/selfLink preserved.** |
| JSON Patch (`application/json-patch+json`) | ✔ | c02 jsonpatch |
| **HistoricalLocation auto-creation** | ✔ | **c02 `test_historical_location_auto_creation`** |
| DELETE + 404 after + cascade matrix | ✔ | c02 |
| Validation errors (missing mandatory / malformed / bad link) | ✔ | c02 |

## 12. Error handling / control info & status codes (Decl=✔)

| Item | Decl | Cover |
|---|:--:|---|
| Non-existent id → 404; unknown collection/property → 4xx | ✔ | c01 (api-fixed earlier) |
| `@iot.id` / `@iot.selfLink` / `<nav>@iot.navigationLink` | ✔ | c01 |
| **status-code: 200 on valid request** | ✔ | **c01 `test_*_returns_200`** |
| **query-status-code: 400 on malformed `$filter`/`$orderby`/`$top`/`$skip`/unknown fn (not 500)** | ✔ | **c01 `test_*_returns_400`, `test_400_body_is_not_stacktrace`** |

## 13. Data Array extension (Decl=✔ data-array/data-array)

| Item | Decl | Cover |
|---|:--:|---|
| GET `?$resultFormat=dataArray` — nav path, strict shape (no `json` wrapper, list-of-rows, `dataArray@iot.count`) | ✔ | **data_array `test_data_array_read_structure`/`_values_ds1`/`_values_ds2` (api-fixed [DA1])** |
| GET collection path `/Observations?$resultFormat=dataArray` (per-datastream grouping, real count) | ✔ | **data_array `test_data_array_collection_path` (api-fixed [DA2]: was 1 hardcoded row)** |
| GET dataArray + `$top` | ✔ | **data_array `test_data_array_read_top` (api-fixed [DA3]: `$top=1`→0)** |
| GET dataArray + `$orderby` | ✔ | **data_array `test_data_array_read_orderby` (api-fixed [DA4]: was 500)** |
| POST `/CreateObservations` (Data Array create) | ✔ | data_array `test_create_observations_*` |

---

## 14. OUT-OF-SCOPE (in B / FROST but NOT in 18-088 C — listed, not implemented)

| Item | Why excluded |
|---|---|
| `contains(p0,p1)` string predicate | OData-4.01 alias, NOT in 18-088 Table 23 (the spec uses `substringof`); istSOS4 → 400. Was `xfail`; removed this round — out of conformance scope, not asserted. |
| Allen temporal fns: `before after meets during overlaps starts finishes` | Not in 18-088 Table 23; istSOS4 → 400 |
| Lambda operators `any()` / `all()` | OData feature, not in STA 1.1 core; istSOS4 → 400 |
| `date()` with timezone 2nd arg; duration-literal arithmetic | Extension; base forms in scope |
| `$format=GeoJSON` | Non-standard output format; istSOS4 → 400 |
| `eq null` / `ne null`; `resultQuality` FROST-shaped filters | OData null semantics / FROST data shape |
| FROST extension test files (`JsonProperties*`, `ResultTypes*`, `Additional*`, `DeleteFilter*`, MultiDatastream, actuation/tasking); `*10.java` v1.0 variants | Beyond 18-088 core Sensing/CUD/Filtering |

---

## 15. Source fixes this round (api-fixer; sources only, tests never weakened)

All tagged `# conformance: <req-id>` in source. Reproduced via curl before/after.

| # | Violation (declared class) | Fix | File(s) |
|---|---|---|---|
| A | `substringof(p0,p1)` returned `[]` (matched on equality) | translate to `p1 LIKE '%'||p0||'%'` | `sta2rest/filter_visitor.py` |
| B | `$count=true` on empty set omitted `@iot.count` | always emit `@iot.count` (0 when empty) | `v1/endpoints/read/read.py` |
| C | `geo.length(geography'LINESTRING(...)')` → 400 `'Geography' has no attribute 'name'` | handle literal-geography branch vs property | `sta2rest/filter_visitor.py` |
| D | PUT full-replace → 405 | new PUT routes; full-replace semantics (missing mandatory→400, omitted optional→null, id/selfLink preserved) | new `v1/endpoints/update/put.py` + thing/location/sensor/observed_property/datastream/observation/feature_of_interest/historical_location routes + `utils/utils.py` |
| DA1 | dataArray nav path wrapped each group in non-spec `json` key | emit group object directly | `sta2rest/visitors.py` |
| DA2 | dataArray collection path returned 1 flat row + hardcoded `count:1` (dropped 15/16 obs) | group per datastream, emit all rows, real count | `sta2rest/visitors.py` (+`sta_parser/lexer.py`) |
| DA3 | dataArray nav `$top=1` → 0 rows | fix off-by-one in row aggregation slice | `sta2rest/visitors.py` |
| DA4 | dataArray + `$orderby` → 500 | fold orderby into per-group row aggregation | `sta2rest/visitors.py` |
| E | `FeaturesOfInterest(id)/Observations` → 500 (route passed a mis-named kwarg to the Observation insert; URL FoI id ignored in the create branch) | align the kwarg + link the URL FoI in the create branch — 201 with the Observation linked to the path FoI (req/create-update-delete/create-entity) | `v1/endpoints/create/observation.py`, `v1/endpoints/create/functions.py` |
| P1–P4 | error-handling: DB-down → 503; client/server split (4xx for bad input vs 500 internal, no stacktrace); bad FK / `@iot.id` → 400; shared error helper for `{code,type,message}` | mirror the read path on write endpoints; controlled error bodies | write endpoints under `v1/endpoints/`, `utils/` |

No regressions: all previously-passing tests remain green; the 4 ex-`xfail`
([A][B][C][D]) are now positive green tests.
