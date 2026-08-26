# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The v1 Swagger UI landing description.

Kept out of api.py so a ~100-line markdown block doesn't bury the router
wiring. This is also the ONLY place several GSoC deliverables can be
represented in Swagger at all -- the append-only AuditLog, the row-level
security policies, and the public-access read behavior are a table, a set
of stored Postgres functions, and a WHERE clause, not endpoints. There is
no operation to attach a summary/description to for any of them.
"""

V1_DESCRIPTION = """
A SensorThings API implementation in Python, extended with a full
authentication, authorization, and audit layer.

*This page documents that extension end to end: how to authenticate, how
access is staged and enforced, and where the non-endpoint parts of it —
row-level security, the audit trail — actually live.*

---

### Quick start

1. Press **Authorize** (top right) and sign in with a local account —
   username/password, e.g. the seeded `administrator` account. Swagger
   posts to `POST /Login` and stores the bearer token for you.
2. Every operation with a padlock icon then sends
   `Authorization: Bearer <token>` automatically when you run **Try it out**.
3. No token isn't rejected outright — an anonymous request runs as the
   PostgreSQL `guest` role and sees only rows a row-level-security policy
   has marked public.

---

### The trust model

Access is staged, not binary. No path skips a stage.

| State | How you get there | What you can do |
|:--|:--|:--|
| **guest** | no token at all | read rows where `is_public` is true |
| **pending** | `POST /Register`, or a first-time login via `GET /auth/{provider}/login` | nothing — every authenticated route returns 403 |
| **approved** | an administrator calls `PATCH /Users/{id}/policy-approval` | whatever the granted RBAC role and row-level-security policy allow |
| **rejected** | an administrator calls `PATCH /Users/{id}/reject` | nothing, permanently, unless the applicant re-registers |

Registering and logging in via an external identity provider are both
**requests**, not grants — neither ever issues a usable access token by
itself. An administrator always makes the decision.

---

### RBAC roles

Assignable: `viewer` &nbsp;·&nbsp; `editor` &nbsp;·&nbsp; `obs_manager` &nbsp;·&nbsp; `sensor` &nbsp;·&nbsp; `qc` &nbsp;·&nbsp; `odrl_governed`

`administrator` is deliberately **not** assignable through this API at
all — promotion to admin is infrastructure/DBA-only, and the last
remaining administrator can never be demoted (`PATCH /Users/{id}/role`
returns `409`). `pending` is an internal state and can never be set
directly by a caller.

---

### Row-level security

Each role maps to a stored PostgreSQL policy function — `viewer_policy`,
`editor_policy`, `obs_manager_policy`, `sensor_policy`, `qc_policy`.
Enforcement happens **inside the database**, not in this application's
Python code: the API switches the session's active role with
`SET LOCAL ROLE` per request, and PostgreSQL's own row-level security does
the filtering.

`odrl_governed` is the one role with a genuinely per-dataset predicate
(`dataset_id = ...`) rather than a blanket grant, built for dataset-scoped
access requests and applied directly by `PATCH /Users/{id}/policy-approval`.

---

### Audit log

There is no endpoint for this, by design. `AuditLog` is append-only at the
database level — `UPDATE`/`DELETE` are revoked from every role, including
`administrator` — and every write happens inside the same transaction as
the action it records, so an action and its audit row commit or roll back
together.

Recorded action types: `PUBLIC_READ` &nbsp;·&nbsp; `RESTRICTED_REQUEST` &nbsp;·&nbsp; `ADMIN_APPROVAL` &nbsp;·&nbsp; `ADMIN_REJECTION`

---

### External identities

Google, Microsoft, GitHub, ORCID, and SWITCH edu-ID are supported through
one provider-parameterized pair of routes rather than five separate
implementations — see **External Authentication** below, including why
those two routes can't be exercised from this page.

A provider only appears as usable if its `*_CLIENT_ID` /
`*_CLIENT_SECRET` environment variables are configured for this
deployment; an unconfigured provider name returns `404`.

---

### Error bodies

Three different shapes exist across this API, for historical reasons.
Each operation below documents the one it actually returns — check the
listed response codes rather than assuming:

| Shape | Origin |
|:--|:--|
| `{"detail": "..."}` | raised as `HTTPException` |
| `{"message": "..."}` | built inline by a handler |
| `{"code": 400, "type": "error", "message": "..."}` | the canonical SensorThings error body |

A fourth shape, `{"detail": [{"loc", "msg", "type"}]}`, is FastAPI's
standard validation error (`422`) and is generated automatically wherever
a request body or parameter is typed.
"""
