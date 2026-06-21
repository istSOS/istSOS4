# OGC SensorThings API v1.1 — Conformance Test Suite (istSOS4)

Black-box HTTP conformance tests for istSOS4's STA v1.1 endpoint, covering the
**three OGC 18-088 conformance classes** — three marked classes, organized into
per-class subfolders (**18 test files, 396 tests**). `tests/conformance/` holds
**only OGC 18-088**; FROST extensions (Data Array, Filtered Delete, Network) live
under `tests/extensions/` (see below):

| Marker | Class | Folder / files | Owner |
|---|---|---|---|
| `c01` | Sensing Core | `c01/` — service_root, read_entities, navigation, properties, refs, errors (**203**) | c01-sensing-core |
| `c02` | Create-Update-Delete | `c02/` — create, deep_insert, update_patch, update_put, delete, validation, jsonpatch (**73**) | c02-cud |
| `c03` | Filtering Extension | `c03/` — query_options, filter_logic_arith, filter_string, filter_datetime, filter_geo (**120**) | c03-filtering |

Each file is owned by a single author agent (no cross-writing). Tests are marked
per-test with `@pytest.mark.c01/c02/c03`.

> **Extensions** (FROST/proprietary, NOT 18-088) live in a **separate** tree,
> `tests/extensions/` — **not** part of this 396-test total: `data_array` (**9**)
> and `filtered_delete` (**7**; implemented but **deliberately not declared** in
> `serverSettings` — a mass bulk-delete kept un-announced) under `NETWORK=0`, and
> `network` (**30**) under `NETWORK=1` (the two configs can't run in one pass).

The shared scaffolding (`conftest.py`, `client.py`, `sample_data.py`, `pytest.ini`)
stays in the suite **root** (`tests/conformance/`); the per-class subfolders
inherit the root fixtures. `pytest.ini` sets **`--import-mode=importlib`** (so
identically-named helpers across subfolders never collide and no `__init__.py`
packaging is required) and **`norecursedirs = .venv __pycache__ .pytest_cache`**
(keeps collection out of the virtualenv/caches).

The `seed` fixture loads `tests/docs/entitiesDefault.json` exactly; the per-URI
declared-vs-covered ledger (sets A/B/C) is `tests/docs/COVERAGE_MATRIX.md`, and the OGC
TeamEngine request set (set A) is `tests/docs/ENGINE_REQUESTS.txt`. Full results live in
`CONFORMANCE_REPORT.md` (this folder). Shared scaffolding is owned by the conformance
lead; test files are owned per the table above.

## Install

The tests need `pytest`, `httpx`, `pytest-xdist`. Use an isolated venv:

```bash
uv venv tests/conformance/.venv --python 3.12
uv pip install --python tests/conformance/.venv/bin/python -r tests/conformance/requirements.txt
```

(`pip -r tests/conformance/requirements.txt` into any venv works too.)

## Run

The istSOS4 API must be running. Default target:
`http://localhost:8018/v4/v1.1` (override with `STA_BASE_URL`).

```bash
PYBIN=tests/conformance/.venv/bin/python

# one class at a time (three 18-088 classes)
$PYBIN -m pytest tests/conformance -m c01          # 203 passed
$PYBIN -m pytest tests/conformance -m c02          # 73 passed
$PYBIN -m pytest tests/conformance -m c03          # 120 passed

# or a single subfolder
$PYBIN -m pytest tests/conformance/c01

# whole suite, in parallel (isolation must hold) -> 396 passed (18-088 only)
$PYBIN -m pytest tests/conformance -n auto

# point at another deployment
STA_BASE_URL=http://host:port/v4/v1.1 $PYBIN -m pytest tests/conformance
```

`pytest tests/conformance` auto-selects this directory's `pytest.ini` (not the
repo-root one), so no app/asyncio bootstrap is pulled in.

## Fixtures (from `conftest.py`)

- **`base_url`** (session) — `STA_BASE_URL` or the default.
- **`client`** (session) — an `STAClient` (see `client.py`): `get/post/patch/put/delete`,
  plus `collection`, `values`, `by_id`, `nav`, `follow_self_link`, `create`,
  `location_of`. URL joining and query encoding are handled for you; absolute
  links the server emits are passed through. Verbs return raw responses (assert
  status yourself); `*_json` helpers raise on non-2xx.
- **`seed`** (session, READ-ONLY) — deep-inserts one known subtree and yields a
  `SeedData` (ids, names, `results`, `phenomenon_times`, `n_observations`).
  Deletes everything on teardown. Use it from **c01/c03 only**; never mutate it.
- **`unique_name`** (function) — a **factory**: call `unique_name()` (optionally
  `unique_name("prefix")`) to get a fresh UUID-tagged string for collision-free
  created entities. c02 uses this and cleans up what it creates.

## Critical: the database is NOT empty

istSOS4 already holds thousands of unrelated rows. **Collection-wide counts are
not predictable.** Filter/paging tests (c03) must scope to the seeded subtree —
navigate from the seed datastream (`/Datastreams(<seed.datastream_id>)/Observations?...`)
or combine with the unique tag (`...&$filter=startswith(name,'<seed.tag>')`).
See the expectation table in `sample_data.py`.

## Conventions

- One assertion target per test where practical (better xdist parallelism +
  clearer failure reports). Assert status code **and** body shape.
- Every test docstring cites the 18-088 requirement it covers.
- No hard-coded ids/self-links/counts — derive them at runtime.
- Authors assert against the **standard**, never against istSOS4's current
  behaviour. Real spec violations are escalated to the lead, not silenced.
