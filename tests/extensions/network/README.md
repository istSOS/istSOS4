# NETWORK extension test suite (istSOS4 proprietary — NOT OGC 18-088)

Tests for istSOS4's **NETWORK** extension: a `Network(name)` entity that relates
to `Datastream` exactly like Thing/Sensor/ObservedProperty — a **mandatory
many-to-one** (a Datastream belongs to one Network; a Network has many
Datastreams). This is proprietary istSOS4 behaviour and is **kept entirely
separate from the conformance suite** (`tests/conformance/`), which is OGC 18-088
and runs with `NETWORK=0`.

## Requires `NETWORK=1`

These tests only pass against a service started with the **`NETWORK=1`** feature
flag (so the `Network` table, `Datastream.network_id NOT NULL`, the `/Networks`
routes and the `Network@iot.navigationLink` exist). They will fail against a
default (`NETWORK=0`) deployment — and conversely the 399-test conformance suite
does not run under `NETWORK=1` (its seed has no `network_id`). The two suites live
in separate worlds by design.

Set it in `.env` (`NETWORK=1`) and **restart the API container** (env vars are
read at process start; `uvicorn --reload` reloads source only). The DB is rebuilt
with the network schema when the flag is on.

## Run

```bash
PY=tests/conformance/.venv/bin/python      # reuses the same isolated venv (httpx + pytest)

# the whole extension tree (only network tests exist today)
$PY -m pytest tests/extensions -m network -q

# or just this folder
$PY -m pytest tests/extensions/network -q

# point at another deployment
STA_BASE_URL=http://host:port/v4/v1.1 $PY -m pytest tests/extensions -m network
```

Expected: **19 passed** (all green).

## Layout & isolation

```
tests/extensions/
├── pytest.ini      # marker `network`, --import-mode=importlib (separate from conformance)
├── conftest.py     # base_url / client / unique_name  (does NOT inherit the conformance conftest)
├── client.py       # copy of the conformance STA client
└── network/
    ├── conftest.py            # network_seed fixture (the dataset)
    ├── test_network_read.py
    ├── test_network_navigation.py
    └── test_network_create.py
```

- Marker: **`@pytest.mark.network`** only (never `c01/c02/c03/data_array`).
- `tests/extensions/` is a **sibling** of `tests/conformance/`, so the conformance
  `conftest.py`/`seed` is never inherited (it would fail under `NETWORK=1`).
- No conformance file or application source is touched — TESTS only.

## `network_seed` (read-only dataset)

Tag-scoped (the shared DB is not assumed empty); created via deep-insert and torn
down completely:

- **Network A** ← 2 Datastreams (results `[3,4]` and `[5]`)
- **Network B** ← 1 Datastream (result `[6]`)
- one Thing (+ 1 Location for FeatureOfInterest auto-generation) owns all 3
  Datastreams; each has its own Sensor + ObservedProperty + Observations.

## What is covered

**Read** (`test_network_read.py`): collection GET; entity-by-id; control info
(`@iot.id`, absolute `@iot.selfLink`, `Datastreams@iot.navigationLink`); selfLink
resolves; `$select=name`; `$filter=name eq`.

**Navigation** (`test_network_navigation.py`): many-to-one `/Datastreams(id)/Network`
(+`/$ref`); one-to-many `/Networks(id)/Datastreams` (+`/$ref`); grouping is
per-network (A vs B exact id/name sets); the Datastream exposes
`Network@iot.navigationLink`; `$expand=Network`; relation `$filter=Network/name eq`.

**Create / mandatoriness** (`test_network_create.py`): `POST /Networks`;
deep-insert a Thing whose Datastream carries `Network` inline; deep-insert a
Network with nested `Datastreams`; **Network is mandatory** — a Datastream create
without it → `400 "Missing required properties 'Network'"` (like omitting
Sensor/ObservedProperty).

## Known behaviour found while writing these tests

- **Network resource path is `/Networks`** (plural); the Network's datastreams
  nav link is advertised as `/Network(id)/Datastreams` (singular) but
  `/Networks(id)/Datastreams` resolves correctly (used by the tests).
- **Direct create with a Network link works** (api-fixed). Previously
  `create/datastream.py` (and `update/datastream.py`) added `"Network"` to its
  allowed keys under `if AUTHORIZATION`, not `if NETWORK`, so a direct
  `POST /Datastreams` with a `Network` key was wrongly rejected
  (`400 "Invalid keys in payload: Network"`). That gating is now `if NETWORK:`, so
  `POST /Datastreams` and `POST /Things(id)/Datastreams` with a `Network {@iot.id}`
  link return **201** (tested by `test_direct_post_datastream_with_network_link`),
  alongside the deep-insert paths.
- **`POST /Networks(id)/Datastreams` → 405** (no such route): a datastream is not
  created under the Network's navigation link. This is left as-is by design
  (lead decision) — create a datastream with a `Network` link via `POST
  /Datastreams` / `POST /Things(id)/Datastreams`, or via deep-insert. Not enabled
  in this round; revisit only if full nav-link-POST parity is wanted.
- With `NETWORK=1`, the Datastream representation gains a proprietary
  `Network@iot.navigationLink` (an extra relation beyond 18-088) — expected for
  this extension, and the reason these tests are **not** part of the conformance
  suite.
