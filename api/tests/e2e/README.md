# End-to-end test: external (OIDC) authentication + network-scoped access

This exercises the **full external-auth pathway** — `/auth/{provider}/login`
→ provider authorize → `/auth/{provider}/callback` → token exchange →
id_token/JWKS validation → pending → admin activation → JWT → **scoped
reads of Datastreams and Observations** — with a real authorization-code
round trip against a fake OpenID Connect provider.

Only the *identity* is canned. Authlib inside the istSOS API does a genuine
code exchange, fetches the discovery document, and validates the
RS256-signed `id_token` against the fake's JWKS, exactly as it would with
Google/Microsoft/ORCID/edu-ID.

## Pieces

| Path | What it is |
|---|---|
| `fake_oidc/provider.py` | ~150-line FastAPI OIDC provider — discovery, `/authorize`, `/token`, `/userinfo`, `/jwks`. Issues real signed id_tokens. |
| `fake_oidc/Dockerfile` | container for it |
| `../../../docker-compose.e2e.yml` | adds the `oidc-fake` service and points the API's `google` client at it |
| `run_oidc_e2e.py` | the driver — spins the browser round trip with `requests`, asserts scoped counts against DB ground truth |

## Run

```bash
cd <repo root>

# 1. bring up the normal stack + the fake provider
docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --build

# 2. wait for istsos4-database healthy and dummy data loaded
#    (docker compose logs -f dummy_data  — until it exits)

# 3. run the test
python api/tests/e2e/run_oidc_e2e.py

# 4. tear down
docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v
```

Needs `requests` on the host (`pip install requests`). Uses `docker exec
istsos4-database psql` for ground-truth counts.

## What it asserts

1. First OIDC callback for an unknown identity → **202**, a `pending` user
   row is created with the requested Network stored on it.
2. A second OIDC login while still pending → **202** (no token).
3. Admin `PATCH /Users/{id}/policy-approval {role: viewer, dataset: <network>}` →
   **200**; the row is now `viewer` scoped to that network.
4. OIDC login again → **200 + a real access token**.
5. With that token: `GET /Datastreams` count **== that network's datastream
   count** (a strict subset of the total), same for `/Observations`, and
   **every** datastream returned belongs to that one network.
6. The OIDC viewer cannot write (PATCH → 4xx).

## Configuration

The fake's identity is per-process, from env (set on the `oidc-fake`
service in `docker-compose.e2e.yml`):

- `FAKE_OIDC_SUB` (default `fake-sub-001`)
- `FAKE_OIDC_EMAIL` (default `oidctester@example.com`)
- `FAKE_OIDC_NAME` (default `OIDC Tester`)

To test a second distinct identity, restart `oidc-fake` with a different
`FAKE_OIDC_SUB` and re-run.

## Note on `oidc_providers.py`

The test relies on `GOOGLE_DISCOVERY_URL` being env-overridable (added
alongside the existing `MICROSOFT_DISCOVERY_URL` / `ORCID_DISCOVERY_URL` /
`EDUID_DISCOVERY_URL` overrides). That override is also what lets a real
deployment point `google` at a private Keycloak or a staging endpoint.
