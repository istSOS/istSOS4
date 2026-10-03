# tests/connector/validate

Conformance checks for the connector's *generated output* against the
official DCAT-AP 3.0.0 and STAC 1.0 schemas. Unlike the rest of
`tests/connector`, these hit a real running connector instance rather than
mocking `app` internals.

## Running

    pytest -v tests/connector/validate

Requires a connector reachable at `CONNECTOR_BASE_URL`. The DCAT-AP SHACL
shapes and the connector's DCAT/STAC output are fetched into `./turtle/`
automatically on first run -- no manual setup step. If the connector isn't
reachable, tests skip (not fail) so this doesn't break unrelated CI runs.

## Config (env vars)

| Var                  | Default                                              | Meaning                                   |
|-----------------------|-------------------------------------------------------|--------------------------------------------|
| `CONNECTOR_BASE_URL`   | `http://localhost:8018/istsos4/v1.1/connector`         | Root of the connector router (`{HOSTNAME}{SUBPATH}{VERSION}/connector`, per `app/__init__.py`/`app/main.py` defaults) -- override the whole value if your deployment sets `HOSTNAME`/`SUBPATH`/`VERSION` differently |
| `CONNECTOR_TIMEOUT_S`  | `30`                                                    | HTTP timeout for downloads                 |
| `CONNECTOR_NETWORKS`   | `1,2`                                                  | Comma-separated `network_id` integers to expect subcatalogs for (matches the `int` type on `/dcat/{network_id}` and `/stac/{network_id}`) |

`turtle/` is gitignored -- it's fetched fresh, never committed.

## What's fetched

- `dcat/root.ttl` -- always required. A 404 here means `DCAT_TRANSFORMER=0`
  on the connector, and the whole suite skips (nothing to validate).
- `dcat/orphan.ttl` and `dcat/{network_id}.ttl` for each id in
  `CONNECTOR_NETWORKS` -- optional. A 404 on these is a legitimate "this
  scope doesn't exist here" (e.g. `NETWORK=0`, or that network id isn't in
  the DB), so it's skipped per-file rather than failing the fixture.
- `dcat-ap-SHACL.ttl` -- the official DCAT-AP 3.0.0 shapes from SEMICeu,
  cached for a week.
- STAC conformance reads `{CONNECTOR_BASE_URL}/stac` directly via pystac's
  validator; nothing from it is written to `turtle/`.