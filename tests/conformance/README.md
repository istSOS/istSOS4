# OGC SensorThings API v1.1 — Conformance Test Suite (istSOS4)

Black-box HTTP conformance tests for istSOS4's STA v1.1 endpoint, covering the
three core conformance classes:

| Marker | Class | File(s) | Owner |
|---|---|---|---|
| `c01` | Sensing Core | `test_c01_sensing_core.py` | c01-sensing-core |
| `c02` | Create-Update-Delete | `test_c02_cud.py`, `test_c02_jsonpatch.py` | c02-cud |
| `c03` | Filtering Extension | `test_c03_filtering.py`, `test_c03_filter_logic_arith.py`, `test_c03_filter_string.py`, `test_c03_filter_datetime.py`, `test_c03_filter_geo.py` | c03-filtering |

Each file is owned by a single author agent (no cross-writing). Tests are marked
per-test with `@pytest.mark.c01/c02/c03`, so the split needs no `pytest.ini`
change. The `seed` fixture loads `entitiesDefault.json` exactly; gap analysis is
in `docs/COVERAGE_MATRIX.md` (sets A/B/C) and `docs/ENGINE_REQUESTS.txt` (set A).

The authoritative scope/checklist is **`docs/CONFORMANCE_PLAN.md`** — read it first.
Shared scaffolding (`conftest.py`, `client.py`, `sample_data.py`, `pytest.ini`)
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

# one class at a time
$PYBIN -m pytest tests/conformance -m c01
$PYBIN -m pytest tests/conformance -m c02
$PYBIN -m pytest tests/conformance -m c03

# whole suite, in parallel (isolation must hold)
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
