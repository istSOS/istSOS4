# OGC SensorThings API v1.1 — Conformance Report (istSOS4)

Black-box conformance suite for istSOS4's STA v1.1 endpoint, covering the three
core conformance classes plus the Data Array extension, against everything the live
`serverSettings.conformance` declares. Per-URI ledger + gap analysis:
`tests/docs/COVERAGE_MATRIX.md`. Engine request set: `tests/docs/ENGINE_REQUESTS.txt`.

**Target:** `http://localhost:8018/v4/v1.1` (override `STA_BASE_URL`).
**Standard:** OGC 18-088 — SensorThings API Part 1: Sensing v1.1.

## Result

| Suite | Class | Files | Passed | xfail | Failed |
|---|---|---|---:|---:|---:|
| `-m c01` | Sensing Core | `c01/` — service_root, read_entities, navigation, properties, refs, errors | 203 | 0 | 0 |
| `-m c02` | Create-Update-Delete | `c02/` — create, deep_insert, update_patch, update_put, delete, validation, jsonpatch | 73 | 0 | 0 |
| `-m c03` | Filtering Extension | `c03/` — query_options, filter_logic_arith, filter_string, filter_datetime, filter_geo | 120 | 0 | 0 |
| `-m data_array` | Data Array extension | `data_array/test_data_array.py` | 9 | 0 | 0 |
| **`-n auto` (all)** | | | **405** | **0** | **0** |

All green with **no `xfail`s**: `contains` (an OData-4.01 alias not in 18-088
Table 23) is out-of-scope and is no longer tested (see register below). Parallel
isolation holds. The `seed` fixture
loads the exact compliance dataset and tears it down (verified: zero leftover
entities).

```bash
PY=tests/conformance/.venv/bin/python
$PY -m pytest tests/conformance -m c01          # 203 passed
$PY -m pytest tests/conformance -m c02          # 73 passed
$PY -m pytest tests/conformance -m c03          # 120 passed
$PY -m pytest tests/conformance -m data_array   # 9 passed
$PY -m pytest tests/conformance -n auto         # 405 passed
```

## What changed this round

`serverSettings.conformance` was **expanded** and now declares, among others,
`built-in-query-functions`, `count`, `pagination`, `status-code`,
`query-status-code`, `update-entity-put`, `historical-location-auto-creation`
and `data-array/data-array`. Declaring a class is a promise: every URI must have
a **passing** test. Four prior `xfail`s had been justified by "class not
declared" — that justification became false, so each was either fixed in source
and converted to a positive test, or (for `contains`) re-justified on
Table-23 grounds. Baseline before that round: **350 passed / 5 xfailed**, ending at
zero xfails. The suite has since grown to **405 passed / 0 xfailed** (see *Latest
changes* below). It is organized into per-class subfolders
(`c01/ c02/ c03/ data_array/`) under `tests/conformance/`; the shared fixtures
(`conftest.py`, `client.py`, `sample_data.py`) stay at the root and are inherited.

- **4 ex-`xfail`s → green positive tests** (after source fixes [A][B][C][D]):
  `substringof`, empty-set `@iot.count:0`, `geo.length` on a literal geography,
  and PUT full-replace.
- **New coverage for newly-declared classes:** pagination (full `@iot.nextLink`
  traversal), `status-code`/`query-status-code` (200 valid / 400 malformed, never
  500), `historical-location-auto-creation`, and the 16 `datamodel/*/properties`
  + `datamodel/*/relations` URIs (both directions).
- **New `data_array` class** (`data_array/test_data_array.py`, 9 tests) covering GET
  `?$resultFormat=dataArray` (nav + collection, `$top`, `$orderby`) and POST
  `/CreateObservations` — after a 4-part read-path fix (DA1–DA4).

## API changes this round (source only; tests never weakened)

Reproduced via curl before/after; each tagged `# conformance: <req-id>` in source.

| # | Violation (declared class) | Root cause & fix | Files |
|---|---|---|---|
| A | `substringof(p0,p1)` returned `[]` for a valid substring (matched on equality) | translate to SQL `p1 LIKE '%'||p0||'%'` (req/request-data/built-in-query-functions) | `sta2rest/filter_visitor.py` |
| B | `$count=true` on an empty set omitted `@iot.count` | always emit `@iot.count` (= 0 when empty); non-empty unchanged; `$count=false` still omits (req/request-data/count, §9.3.4 Req 28) | `v1/endpoints/read/read.py` |
| C | `geo.length(geography'LINESTRING(...)')` → 400 `'Geography' object has no attribute 'name'` | branch on literal-geography vs property in the geo-function resolver | `sta2rest/filter_visitor.py` |
| D | PUT full-replace → 405 | new PUT routes with full-replace semantics: missing mandatory → 400, omitted optional → null, `@iot.id`/selfLink preserved, mandatory relations kept valid (req/create-update-delete/update-entity-put, §10.3) | new `v1/endpoints/update/put.py` + thing/location/sensor/observed_property/datastream/observation/feature_of_interest/historical_location routes + `utils/utils.py` |
| DA1 | dataArray nav path wrapped each group in a non-spec `json` key | emit the group object directly (req/data-array/data-array) | `sta2rest/visitors.py` |
| DA2 | dataArray collection path returned a single flat row + hardcoded `dataArray@iot.count:1`, dropping all but one observation per datastream | group per datastream, stream all observations as list-of-rows with the real per-group count | `sta2rest/visitors.py` (+`sta_parser/lexer.py`) |
| DA3 | dataArray nav `$top=1` → 0 rows (off-by-one) | fix row-aggregation slice | `sta2rest/visitors.py` |
| DA4 | dataArray + `$orderby` → 500 Internal Server Error | fold the orderby into the per-group row aggregation | `sta2rest/visitors.py` |

No regressions — every previously passing test remains green.

## Latest changes (error-handling + POST-to-navigation-link)

Two further batches landed after the round above (source + tests; tests never weakened):

- **Error-handling hardening (P1–P4):** DB-unavailable now returns **503** (write
  endpoints mirror the read path) instead of a misleading 400; client vs server faults
  are split cleanly (**4xx** for bad input, **500** only for genuine internal errors,
  never a stacktrace); a bad foreign-key / `@iot.id` reference returns **400** (not 500);
  responses go through a shared error helper for consistent `{code,type,message}` bodies.
- **POST-to-navigation-link create (Req 33), +6 c02 tests:** creating the child under a
  parent nav-link is now covered for `Things(id)/Datastreams`, `Sensors(id)/Datastreams`,
  `ObservedProperties(id)/Datastreams`, `Locations(id)/Things`,
  `Things(id)/HistoricalLocations` (edge), and `FeaturesOfInterest(id)/Observations` —
  each asserts 201 + `Location` and verifies the link in both directions.
- **Fix `FeaturesOfInterest(id)/Observations` 500 → 201:** the route passed a mis-named
  kwarg to the Observation insert (singular vs plural) and the create branch ignored the
  URL FoI id; both fixed, so the Observation is created and linked to the URL's
  FeatureOfInterest (regression-checked: `Datastreams(id)/Observations` and FoI
  auto-generation still 201; missing-Datastream → 400).

**Net:** c02 **62 → 73**; whole suite now **405 passed / 0 xfailed** (`-n auto`).

## xfail register

**None.** The suite has zero `xfail`s.

`contains` (an OData-4.01 alias **not** in 18-088 §9.3.3.5.2 Table 23 — the spec's
substring predicate is `substringof`, which is implemented and tested) was the
last `xfail`; it has been **removed** as out-of-conformance-scope (istSOS4 returns
400 `Unknown function: contains`; not a required function of any declared class).
See `tests/docs/COVERAGE_MATRIX.md` §14.

The four previously-registered `xfail`s (`test_put_replace_thing`,
`test_count_empty_set_returns_zero`, `test_substringof`,
`test_geo_length_literal_linestring`) are now **positive passing tests** — their
"class not declared" justification was invalidated by the expanded conformance
array and the underlying violations were fixed in source.

## Coverage by class

- **c01 Sensing Core (203)** — service root + conformance; per-collection GET;
  control info; entity-by-id; property access + raw `$value`; `$ref`
  single/collection; navigation both directions across the 2-datastream tree;
  deep resource paths; **datamodel mandatory-properties + relations for all 8
  entities** (`req/datamodel/*/properties` + `*/relations`); **status-code (200
  valid)** and **query-status-code (400 on malformed `$filter`/`$orderby`/`$top`/
  `$skip`/unknown function — never 500)**; 404 error handling.
- **c02 CUD (73)** — POST per collection (201 + Location); deep insert (+ status
  code); link-to-existing via `{"@iot.id"}`; **POST-to-navigation-link create (Req 33)
  for Things/Sensors/ObservedProperties→Datastreams, Locations→Things,
  Things→HistoricalLocations, Datastreams/FeaturesOfInterest→Observations**; FoI
  auto-generation; PATCH partial + link-change; **PUT full-replace** (replace
  semantics, missing-mandatory → 400, omitted-optional → null); **JSON Patch**
  (add/replace/copy/move/remove/test); DELETE + explicit cascade matrix;
  validation errors; **historical-location auto-creation** (deep-insert and
  POST-to-nav both auto-create a linked HistoricalLocation with a time).
- **c03 Filtering (120)** — `$orderby`/`$top`/`$skip`/`$count`(±`false`,
  empty-set→0)/**`@iot.nextLink` full traversal**; `$select` props + navigation;
  `$expand` single/multiple/nested/nested-with-options; `$filter`
  comparison/logical/arithmetic; string fns incl. **`substringof`**; datetime
  fns; math fns; geospatial + all `st_*` relations incl. **`geo.length` literal**;
  relation filters (1-/2-/multi-hop); combinations. Scoped to the seed for exact
  result-sets. No `xfail`s (`contains` removed as out-of-scope).
- **data_array (9)** — GET `?$resultFormat=dataArray` strict spec shape (no `json`
  wrapper, list-of-rows, real `dataArray@iot.count`) on the nav path and the
  collection path, with `$top` and `$orderby`; POST `/CreateObservations`
  (success + missing-result / missing-datastream validation). Created
  observations cleaned up in teardown.

## Extensions (separate suite — not part of the 405)

The **Network** entity is an istSOS4 extension, gated behind `NETWORK=1` and tested by a
**separate** suite under `tests/extensions/network/` (with its own README). It is **not**
part of the 405-test conformance total above, which runs with `NETWORK=0`. Latest Network
result: **27 passed, 2 xfailed** — the two `xfail`s are documented route deviations
(PUT and nav-link-POST on `Networks`) explained in that suite's README.

## Methodology

Coverage was driven by four sets (full matrix in `tests/docs/COVERAGE_MATRIX.md`):
**A** = OGC TeamEngine requests (block-parsed `full_logs.txt` →
`tests/docs/ENGINE_REQUESTS.txt`; query options + the six comparison operators only);
**B** = FROST-Server v2.7.2 c01/c02/c03 tests (read-only, extensions excluded);
**C** = OGC 18-088 (§8/§9/§10 + Table 23); the suite implements `(B ∪ C) − A`
plus everything the expanded `serverSettings.conformance` now declares.

**Seed:** the session-scoped `seed` fixture deep-inserts **`entitiesDefault.json`**
verbatim (1 Thing → 1 Location + 2 Datastreams × [Sensor + ObservedProperty + 2
Observations]; results 3/4/5/6; phenomenonTime 2015-03-03..06; Point(-117.05,
51.05); the intentional typos `Tempretaure`/`Tempreture` preserved) and deletes
everything on teardown. All assertions scope to seed ids so they hold under
`-n auto` (per-worker seeds) and against a non-empty live DB.

**Adjudication:** a deviation on a **declared** URI is a real 18-088 violation →
routed to api-fixer (sources only; tests never weakened). A deviation on a
non-declared item is optional. A 500 is never acceptable. FROST-only items (not
in 18-088) are out-of-scope (matrix §14). File ownership held throughout:
authors wrote only their `test_*` files; the api-fixer touched only `api/app/`
sources; the lead alone routed violations and owns the scaffolding.

## Deliverables

- `tests/conformance/` — root scaffolding (`conftest.py`, `client.py`,
  `sample_data.py`, `pytest.ini`, `requirements.txt`, isolated `.venv`, this report,
  `README.md`) + per-class subfolders `c01/` (6 files), `c02/` (7), `c03/` (5),
  `data_array/` (1) — 19 test files, 405 tests. Subfolders inherit the root
  fixtures; `--import-mode=importlib`.
- `tests/docs/` — `COVERAGE_MATRIX.md`, `EXTENSIONS_ANALYSIS.md`, `ENGINE_REQUESTS.txt`,
  `entitiesDefault.json` (seed dataset).
- `tests/docs/COVERAGE_MATRIX.md` — per-URI ledger + A/B/C/GAP matrix with adjudication.
- API source fixes under `api/app/` (tagged `# conformance:`; committed): this round's
  A,B,C,D + DA1–DA4, plus the error-handling P1–P4 batch and the
  `FeaturesOfInterest/Observations` 500→201 fix.
