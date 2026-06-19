# OGC SensorThings API v1.1 — Conformance Test Plan (shared context)

> This file is the **single source of truth** for every agent in the team.
> Read it fully before writing any code. Do not duplicate its content into other files — reference it.

## Target under test

- **System:** istSOS4 (this repository)
- **Base URL:** `http://localhost:8018/v4/v1.1` (override with env var `STA_BASE_URL`)
- **OpenAPI docs:** `http://localhost:8018/v4/v1.1/docs`
- **Standard:** OGC 18-088 — *SensorThings API Part 1: Sensing v1.1* — https://docs.ogc.org/is/18-088/18-088.html
- **Reference implementation tests (read-only, for inspiration):**
  FROST-Server v2.x, package `de.fraunhofer.iosb.ilt.statests`
  - `c01sensingcore/` → Sensing Core
  - `c02cud/` → Create / Update / Delete
  - `c03filtering/` → Filtering Extension
  GitHub: https://github.com/FraunhoferIOSB/FROST-Server/tree/v2.x/FROST-Server.Tests/src/test/java/de/fraunhofer/iosb/ilt/statests

## Scope and non-goals

In scope — the three core conformance classes only:
1. **Sensing Core** (`c01`)
2. **Create-Update-Delete** (`c02`)
3. **Filtering Extension** (`c03`)

**Out of scope (do NOT port):** FROST-specific extensions and anything beyond OGC 18-088 —
Batch requests, Data Array extension, MultiDatastream, MQTT, custom JSON-property indexing tricks,
`AdditionalTests`, and any FROST `*Extra*`/proprietary behaviour. Use FROST only to discover *which
standard requests* are exercised; re-derive correctness from the OGC standard, not from FROST output.

> **Golden rule:** every assertion must be justifiable against a clause in 18-088. When in doubt,
> `WebFetch` the standard and cite the requirement id (e.g. `req/create-update-delete/...`) in a
> comment above the test.

## The 8 entity sets (Sensing data model)

`Things`, `Locations`, `HistoricalLocations`, `Datastreams`, `Sensors`, `ObservedProperties`,
`Observations`, `FeaturesOfInterest`.

Mandatory properties (verify exact names/values against 18-088 §8 entity tables — do not trust memory blindly):

| Entity | Mandatory properties | Key relations |
|---|---|---|
| Thing | name, description | Locations, HistoricalLocations, Datastreams |
| Location | name, description, encodingType, location | Things, HistoricalLocations |
| HistoricalLocation | time | Thing, Locations |
| Sensor | name, description, encodingType, metadata | Datastreams |
| ObservedProperty | name, definition, description | Datastreams |
| Datastream | name, description, unitOfMeasurement, observationType | Thing, Sensor, ObservedProperty, Observations |
| Observation | result, phenomenonTime\* | Datastream, FeatureOfInterest |
| FeatureOfInterest | name, description, encodingType, feature | Observations |

\* `phenomenonTime`/`resultTime` defaulting and FeatureOfInterest auto-generation from the Thing's
Location are spec behaviours that MUST be tested (see c02).

Control annotations to assert: `@iot.id`, `@iot.selfLink`, `@iot.count`, `@iot.nextLink`,
`<navProp>@iot.navigationLink`. The service root returns `value[]` (name+url per collection) and,
in v1.1, a `serverSettings` object including the `conformance` array.

> **ID type:** do not assume integer ids. Always read the id back from the created entity /
> `@iot.selfLink`. istSOS4 may use integer ids; tests must work regardless of id type.

## Repository / test layout (created by the LEAD before agents start)

Tests are organized into per-class **subfolders** (19 files, 394 tests). The shared
scaffolding stays in the suite root and is inherited by the subfolders.

```
tests/conformance/
├── conftest.py            # fixtures: client, base_url, seeded dataset, cleanup (LEAD, root)
├── client.py              # thin STA client over httpx (get/post/patch/put/delete + helpers)
├── sample_data.py         # canonical payloads + a deep-insert tree used for seeding
├── pytest.ini             # markers + --import-mode=importlib + norecursedirs
├── requirements.txt       # pytest, httpx, pytest-xdist
├── README.md
├── CONFORMANCE_REPORT.md  # full results
├── c01/                   # agent c01: service_root, read_entities, navigation, properties, refs, errors (203)
├── c02/                   # agent c02: create, deep_insert, update_patch, update_put, delete, validation, jsonpatch (62)
├── c03/                   # agent c03: query_options, filter_logic_arith, filter_string, filter_datetime, filter_geo (120)
└── data_array/            # agent dataarray: test_data_array.py (9)
```

Conformance docs (this plan, `COVERAGE_MATRIX.md`, `ENGINE_REQUESTS.txt`,
`REFRACTOR_PLAN.md`, `entitiesDefault.json`) live in **`tests/docs/`**.
`pytest.ini` uses `--import-mode=importlib` (so identically-named helpers across
subfolders don't collide; no `__init__.py` needed) and
`norecursedirs = .venv __pycache__ .pytest_cache`.

**File ownership (to allow safe parallelism — never edit another agent's file):**
- LEAD: root scaffolding (`conftest.py`, `client.py`, `sample_data.py`, `pytest.ini`, `requirements.txt`, `README.md`) + the docs under `tests/docs/`
- c01 agent: `tests/conformance/c01/` only
- c02 agent: `tests/conformance/c02/` only
- c03 agent: `tests/conformance/c03/` only
- dataarray agent: `tests/conformance/data_array/` only
- api-fixer: source code of istSOS4 only (never the tests)

## Shared conventions (all test files)

- **Framework:** `pytest` + `httpx.Client`. No global mutable state.
- **Markers:** `@pytest.mark.c01` / `c02` / `c03` / `data_array`. Registered in `pytest.ini`.
- Each test references the requirement it covers in a docstring, e.g.
  `"""req/request-data/order — $orderby desc on phenomenonTime."""`
- **Isolation:** c02 (create/delete) tests create their own entities and clean up in teardown.
  c01/c03 (read-only) tests rely on the shared seeded dataset (read-only fixture `seed`).
- Naming a unique tag (e.g. a UUID in `name`) to find/clean test entities safely.
- Assert both status code *and* body shape. Prefer specific assertions over `assert resp.ok`.
- No hard-coded ids, self-links, or counts — derive them at runtime.

## conftest.py contract (LEAD implements; agents consume)

Fixtures the LEAD must expose:
- `base_url` (session) — from `STA_BASE_URL` or the default above.
- `client` (session) — configured `httpx.Client(base_url=..., timeout=30)` with helpers from `client.py`.
- `seed` (session) — creates a known dataset via deep insert (1 Thing → Location, 1 Datastream →
  Sensor + ObservedProperty + several Observations across known phenomenonTimes/results/geometries),
  yields a dataclass of created ids, and **deletes everything** on teardown. Read-only for c01/c03.
- `unique_name` (function) — returns a UUID-tagged string for collision-free created entities.

## Definition of done (whole suite)

- `pytest tests/conformance -m c01` / `-m c02` / `-m c03` all green against istSOS4.
- Coverage checklists below fully implemented (or each gap explicitly justified in a `# SPEC-NOTE:`).
- Any failure that reflects a real 18-088 violation is handed to the **api-fixer** (not silenced).
- `pytest tests/conformance -n auto` (xdist) passes — tests are isolated enough to run in parallel.

---

# Coverage checklist — c01 Sensing Core

Read first: 18-088 §8 (data model), §9 (resource path & requests), and FROST `c01sensingcore/`.

1. **Service root document** — `GET /` returns `value[]` with the 8 collection name/url pairs;
   v1.1 `serverSettings.conformance` lists the Core conformance URI.
2. **Each collection GET** — `/Things`, `/Locations`, `/HistoricalLocations`, `/Datastreams`,
   `/Sensors`, `/ObservedProperties`, `/Observations`, `/FeaturesOfInterest` → 200, body has `value[]`.
3. **Control information per entity** — every entity has `@iot.id` and absolute `@iot.selfLink`;
   navigation properties expose `<nav>@iot.navigationLink`.
4. **Entity by id** — `/Things(<id>)` → 200 and matches the entity from the collection.
5. **Property access** — `/Things(<id>)/name` → `{ "name": ... }`.
6. **`$value`** — `/Things(<id>)/name/$value` → raw scalar (no JSON envelope).
7. **Navigation (one-to-many)** — `/Things(<id>)/Datastreams`, `/Datastreams(<id>)/Observations`,
   `/Things(<id>)/Locations`, `/Locations(<id>)/Things`, `/Sensors(<id>)/Datastreams`,
   `/ObservedProperties(<id>)/Datastreams`, `/FeaturesOfInterest(<id>)/Observations`,
   `/Things(<id>)/HistoricalLocations`.
8. **Navigation (many-to-one)** — `/Datastreams(<id>)/Thing`, `/Datastreams(<id>)/Sensor`,
   `/Datastreams(<id>)/ObservedProperty`, `/Observations(<id>)/Datastream`,
   `/Observations(<id>)/FeatureOfInterest`, `/HistoricalLocations(<id>)/Thing`.
9. **Deep resource paths** — `/Things(<id>)/Datastreams(<id>)/Observations`,
   `/Datastreams(<id>)/Thing/Locations`, `/Observations(<id>)/Datastream/ObservedProperty`.
10. **Association links `$ref`** — `/Things(<id>)/Datastreams/$ref` returns only selfLinks;
    `/Observations(<id>)/Datastream/$ref` returns a single selfLink object.
11. **Nested property on related entity** — `/Datastreams(<id>)/Thing/name/$value`.
12. **Top-level `@iot.count` / `@iot.nextLink`** presence semantics on collections.
13. **Error handling** — non-existent id → 404; malformed path → 4xx; property that does not exist → 4xx.
14. **All relations are navigable in both directions** (cross-check with the seeded tree).

---

# Coverage checklist — c02 Create-Update-Delete

Read first: 18-088 §10 (create/update/delete) and FROST `c02cud/`. All requirement ids live under
`req/create-update-delete/...`. Every created entity must be cleaned up.

**CREATE (POST):**
1. POST a minimal valid entity into each of the 8 collections → `201`, `Location` header set,
   body carries `@iot.id` + `@iot.selfLink`; follow-up GET returns it.
2. **Deep insert** — POST a Thing with nested `Locations` and nested `Datastreams` (each with inline
   `Sensor`, `ObservedProperty`, and `Observations`) in one request; verify the whole tree is created
   and linked.
3. **Create with link to existing entity** — POST a Datastream referencing existing Thing/Sensor/
   ObservedProperty via `{"@iot.id": <id>}`.
4. **POST to a navigation link** — `POST /Things(<id>)/Datastreams` (or `/Locations`) creates a
   related entity already linked to the parent.
5. **FeatureOfInterest auto-generation** — POST an Observation without a FeatureOfInterest; the server
   must auto-create/link one from the Thing's Location. Verify `/Observations(<id>)/FeatureOfInterest`.
6. **phenomenonTime / resultTime defaulting** — POST an Observation omitting optional times; assert
   spec-defined defaulting behaviour.
7. **Validation errors** — missing a mandatory property → `400`; bad `@iot.id` link → `4xx`;
   malformed JSON → `400`; wrong/unknown property → `4xx`.

**UPDATE (PATCH):**
8. PATCH a scalar property on each entity type → `200`/`204`; GET confirms the change and that other
   properties are untouched.
9. PATCH a relation (re-link a Datastream to a different Sensor via `{"@iot.id": ...}`).
10. PATCH a non-existent entity → `404`.

**UPDATE (PUT):** (if istSOS4 supports it — 18-088 treats PUT as full replace)
11. PUT to replace an entity; verify replacement semantics, or assert the documented behaviour if PUT
    is not supported (`# SPEC-NOTE`).

**DELETE:**
12. DELETE each entity type → `200`/`204`; subsequent GET → `404`.
13. **Cascade** — deleting a Datastream deletes its Observations; deleting a Thing deletes its
    Datastreams & HistoricalLocations (verify dependent entities are gone).
14. DELETE a non-existent entity → `404`.
15. **Set-to-null / unlink** behaviour where the spec defines it.

---

# Coverage checklist — c03 Filtering Extension

Read first: 18-088 §9.3 (query options) and FROST `c03filtering/` (Capability3Tests, FilterTests,
DateTimeTests, GeoTests, OrderByTests — port the *standard* operators only). Requirement ids under
`req/request-data/...`. Use the seeded dataset; all read-only.

**System query options:**
1. **`$orderby`** — single asc/desc, multiple keys, on different datatypes (number, time, string).
2. **`$top` / `$skip`** — paging windows; correct counts and ordering preserved.
3. **`$count=true`** — `@iot.count` equals the true total irrespective of `$top`.
4. **`@iot.nextLink`** — present when results exceed page size; following it returns the next page
   without overlap/gaps.
5. **`$select`** — single property, multiple properties, and navigation property selection; only
   selected fields (plus control info) returned.
6. **`$expand`** — single nav property; multiple; **nested** (`Datastreams/Observations`); expand
   **with nested query options** e.g. `$expand=Observations($top=2;$orderby=phenomenonTime desc;$select=result)`.

**`$filter` — comparison operators:** `eq, ne, gt, ge, lt, le` (on numeric result, time, string).

**`$filter` — logical:** `and, or, not`, and parenthesised precedence.

**`$filter` — arithmetic:** `add, sub, mul, div, mod` inside comparisons.

**`$filter` — string functions:** `substringof`/`contains`, `startswith`, `endswith`, `length`,
`indexof`, `substring`, `tolower`, `toupper`, `trim`, `concat`.

**`$filter` — date/time functions:** `year, month, day, hour, minute, second, date, time,
totaloffsetminutes, now, mindatetime, maxdatetime`; filtering on `phenomenonTime` instants and
intervals.

**`$filter` — math functions:** `round, floor, ceiling`.

**`$filter` — geospatial functions:** `geo.distance`, `geo.length`, `geo.intersects`, and the
ST_* relations defined by the spec (`st_equals, st_disjoint, st_touches, st_within, st_overlaps,
st_crosses, st_intersects, st_contains, st_relate`) against `Location/location` and
`FeatureOfInterest/feature`. (Confirm which subset 18-088 mandates; mark optional ones.)

**`$filter` across relations:** e.g. `/Datastreams?$filter=Thing/name eq '<x>'`,
`/Observations?$filter=Datastream/ObservedProperty/name eq '<x>'`.

**Combinations:** `$filter` + `$orderby` + `$top` + `$select` + `$expand` together; query options on
expanded sets; `$filter` on an expanded collection.

**Edge cases:** url-encoding, single-quote escaping in string literals, empty result sets,
case-sensitivity per spec.
