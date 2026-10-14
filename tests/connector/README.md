# tests/connector

Tests for the metadata connector in `api/app/v1/connector` (STAC and
DCAT-AP output, auth gate, closed networks, config).

There are two kinds of tests here:

- Unit and router tests (this folder). They need no database, no Redis and
  no Docker. Database and cache calls are mocked.
- `validate/`. These check the real STAC and DCAT-AP output of a running
  connector against the official schemas. They need a running stack. If
  nothing is running they are skipped, not failed.

## Setup

Use Python 3.12 (the same version as `api/Dockerfile`).

From the repo root:

    python -m venv .venv

Activate it:

    Windows cmd:         .venv\Scripts\activate
    Windows PowerShell:  .venv\Scripts\Activate.ps1
    Linux / macOS:       source .venv/bin/activate

Install everything the tests need (this also installs `api/requirements.txt`):

    pip install -r api/requirements-dev.txt

That is the only setup step.

## Run the tests

From the repo root:

    pytest tests/connector

`conftest.py` adds `api/` to `sys.path` itself, so no `cd api` and no
`PYTHONPATH` are needed.

With no stack running, the unit tests pass and the `validate/` tests show up
as skipped, with the reason printed at the end of the run. That is normal.

Run one file, one test, or a keyword:

    pytest tests/connector/test_auth.py
    pytest tests/connector/test_auth.py::test_garbage_token_returns_401
    pytest tests/connector -k closed_network

Run only the unit tests and skip `validate/`:

    pytest tests/connector --ignore=tests/connector/validate

## What each file covers

| File | What it checks |
|------|----------------|
| `test_auth.py` | Auth matrix through the mounted router: 401 and 404 behavior for every combination of `AUTHORIZATION`, `ANONYMOUS_VIEWER`, `OPEN_CATALOG_METADATA` and closed networks, plus bad tokens. |
| `test_auth_gate.py` | The same matrix, calling `auth_gate.gate()` directly with no HTTP layer. |
| `test_config.py` | `CATALOG_CLOSED_NETWORKS` parsing, and turning network names into ids at startup. |
| `test_dcat_gate_parity.py` | STAC and DCAT routes use one shared gate and answer the same way under every flag combination. |
| `test_dcat_root_reveal.py` | The DCAT root only lists closed networks for authenticated callers. |
| `test_error_handling.py` | `catch_errors` and the standard error body. |
| `test_stac_output.py` | STAC schema validity for edge cases the dummy data never produces: live Datastreams, missing geometry, empty names, no units. |
| `validate/` | STAC 1.0 and DCAT-AP 3.0 conformance of a live connector. See `validate/README.md`. |

## Live validation (validate/)

These tests fetch real output from the connector, so the connector code has
to be running. Use the dev compose file, which builds the API from this
checkout:

    docker compose -f dev_docker-compose.yml up -d --build

Do not use the plain `docker-compose.yml` for this. It runs a prebuilt image
from upstream that does not contain the connector, so every connector URL
returns 404 and the validate tests skip with "DCAT_TRANSFORMER appears
disabled".

Things to know before running:

- The connector needs data to harvest. Set `DUMMY_DATA=1` in `.env` before
  the first start. With `NETWORK=1` the generator creates two networks, which
  matches the default `CONNECTOR_NETWORKS=1,2`; with `NETWORK=0` the network
  files are simply skipped. A smaller `N_THINGS` in `.env` makes the first
  start faster.
- `AUTHORIZATION`, `NETWORK` and `DUMMY_DATA` only take effect on a fresh
  database volume, since the schema is created once at first start.
- `STAC_TRANSFORMER=1` and `DCAT_TRANSFORMER=1` must be set in `.env`.
- The first harvest runs right after the API starts. If the tests skip with a
  503 message, wait a bit and run them again.
- The first run downloads the DCAT-AP SHACL shapes from GitHub, so it needs
  internet access. The shapes are cached for a week.

Then:

    pytest -v tests/connector/validate

The connector URL and the list of network ids can be changed with
environment variables. They are listed in `validate/README.md`.

The API is mounted under `/istsos4/v1.1`, so `http://localhost:8018` alone
returns `{"detail":"Not Found"}`. That is expected. The connector root is
`http://localhost:8018/istsos4/v1.1/connector`.

## Troubleshooting

Many failures with "async def functions are not natively supported":
`pytest-asyncio` is not installed. Run `pip install -r api/requirements-dev.txt`.

Two tests skipped with "could not import pyshacl" or "could not import
pystac": same fix, same command.

One async test fails with the same message while the rest pass: that test is
missing `@pytest.mark.asyncio`.

PowerShell refuses to run `Activate.ps1`: run
`Set-ExecutionPolicy -Scope Process RemoteSigned` in that window, then
activate again.

## Writing new tests

- `STAC_TRANSFORMER` and `DCAT_TRANSFORMER` are forced on in `conftest.py`.
  `api.py` reads them once at import time, so they cannot be changed per test.
- To change `AUTHORIZATION`, `ANONYMOUS_VIEWER`, `OPEN_CATALOG_METADATA` or
  the closed networks for one test, use the `set_connector_flags` fixture. It
  patches every module that holds its own copy of those values and undoes the
  change after the test.
- Async tests need `@pytest.mark.asyncio`. `pytest.ini` runs
  `pytest-asyncio` in strict mode, so an async test without the mark fails
  with "async def functions are not natively supported".
