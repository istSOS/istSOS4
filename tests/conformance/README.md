# OGC SensorThings API v1.1 — Conformance Test Suite (istSOS4)

Black-box HTTP conformance tests for istSOS4's STA v1.1 endpoint, covering the
three core conformance classes **plus the Data Array extension** — **four** marked
classes, organized into per-class subfolders (**19 test files, 394 tests**):

| Marker | Class | Folder / files | Owner |
|---|---|---|---|
| `c01` | Sensing Core | `c01/` — service_root, read_entities, navigation, properties, refs, errors (**203**) | c01-sensing-core |
| `c02` | Create-Update-Delete | `c02/` — create, deep_insert, update_patch, update_put, delete, validation, jsonpatch (**62**) | c02-cud |
| `c03` | Filtering Extension | `c03/` — query_options, filter_logic_arith, filter_string, filter_datetime, filter_geo (**120**) | c03-filtering |
| `data_array` | Data Array extension | `data_array/test_data_array.py` (**9**) | dataarray-author |

Each file is owned by a single author agent (no cross-writing). Tests are marked
per-test with `@pytest.mark.c01/c02/c03/data_array`.

The shared scaffolding (`conftest.py`, `client.py`, `sample_data.py`, `pytest.ini`)
stays in the suite **root** (`tests/conformance/`); the per-class subfolders
inherit the root fixtures. `pytest.ini` sets **`--import-mode=importlib`** (so
identically-named helpers across subfolders never collide and no `__init__.py`
packaging is required) and **`norecursedirs = .venv __pycache__ .pytest_cache`**
(keeps collection out of the virtualenv/caches).

The `seed` fixture loads `tests/docs/entitiesDefault.json` exactly; gap analysis is
in `tests/docs/COVERAGE_MATRIX.md` (sets A/B/C) and `tests/docs/ENGINE_REQUESTS.txt`
(set A). The authoritative scope/checklist is **`tests/docs/CONFORMANCE_PLAN.md`** —
read it first. Full results live in `CONFORMANCE_REPORT.md` (this folder); the
error-handling refactor plan is `tests/docs/REFRACTOR_PLAN.md`. Shared scaffolding
is owned by the conformance lead; test files are owned per the table above.

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

# one class at a time (four classes)
$PYBIN -m pytest tests/conformance -m c01          # 203 passed
$PYBIN -m pytest tests/conformance -m c02          # 62 passed
$PYBIN -m pytest tests/conformance -m c03          # 120 passed
$PYBIN -m pytest tests/conformance -m data_array   # 9 passed

# or a single subfolder
$PYBIN -m pytest tests/conformance/c01

# whole suite, in parallel (isolation must hold) -> 394 passed
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
