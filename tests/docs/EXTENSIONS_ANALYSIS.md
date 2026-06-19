# istSOS4 proprietary extensions — analysis (read-only round)

Analysis only: no code/tests written, no `.env` change, extensions remain
OFF-LIMITS for edits until they have their own tests. This documents where each
extension lives, how it activates, how it touches the 8 STA entities, and the two
conformance-critical points (b: extra fields in STA responses; c: role-dependent
result sets).

> **Everything here is flag-aware.** All three extensions are feature-flagged AND
> the database schema itself is gated on the same flags. With a flag OFF the
> extension's tables/columns/routes **do not exist**, so "absent" observations
> below are *trivially* true and do **not** prove the ON behavior. Where the ON
> behavior matters (point b), it is marked **NOT VERIFIABLE (flag OFF)** with the
> flag to enable — I did not enable anything (needs your confirmation + a DB
> rebuild + service restart).

## 0. Feature flags

Read in `api/app/__init__.py` from environment (set in `.env`); the same names are
passed to the database build as `custom.<flag>` GUCs that gate the schema DDL.

| Extension | Flag (`api/app/__init__.py`) | DB gate (SQL) | **Current `.env`** |
|---|---|---|:--:|
| Network | `NETWORK = int(os.getenv("NETWORK", 0))` | `IF current_setting('custom.network')` | **`NETWORK=0`** |
| Authorization | `AUTHORIZATION = int(os.getenv("AUTHORIZATION", 0))` | `IF current_setting('custom.authorization')` | **`AUTHORIZATION=0`** |
| Versioning | `VERSIONING = int(os.getenv("VERSIONING", "0"), 0)` | `custom.versioning` (system_time DDL) | **`VERSIONING=0`** |

Other flags (not extensions): `COUNT_MODE=FULL`, `REDIS=0`, `EPSG=4326`,
`PARTITION_CHUNK=10000`, `VERSION=/v1.1`.

**The 399 conformance tests run in this exact config: NETWORK=0, AUTHORIZATION=0,
VERSIONING=0** — i.e. all three extensions OFF. That is the context in which the
suite is "green". The API connects to Postgres as `ISTSOS_ADMIN` (`.env`:
`ISTSOS_ADMIN=admin`), and because `AUTHORIZATION=0` no `SET ROLE` is performed —
every query runs as that single admin connection.

Evidence (flags OFF): service root exposes exactly the 8 STA collections
(`Datastreams, FeaturesOfInterest, HistoricalLocations, Locations, Observations,
ObservedProperties, Sensors, Things`); `GET /Login` → 404, `GET /Networks` and
`GET /Commits` → 500 (routes unregistered; the read catch-all hits a non-existent
table — a minor flag-OFF robustness nit, not a declared-class concern).

---

## 1. NETWORK (flag `NETWORK`, currently 0)

**Purpose (confirmed):** groups entities into thematic networks (hydrology,
hydrogeology, ecology…). A `Network(id, name)` table; `Datastream` carries a
`network_id` FK. Attached only at the Datastream level — the rest of the tree is
reached through the Datastream. ✅ your description is accurate.

**Where it lives**
- Schema (`database/istsos_schema.sql`, gated `IF current_setting('custom.network')`):
  `CREATE TABLE sensorthings."Network"(id, name)`; `ALTER TABLE "Datastream" ADD
  COLUMN "network_id" BIGINT **NOT NULL** REFERENCES "Network"(id) ON DELETE
  CASCADE`; index; SQL functions `@iot.selfLink(Network)`,
  `Datastreams@iot.navigationLink(Network)`, **`Network@iot.navigationLink(Datastream)`**.
- Models: `api/app/models/network.py` (`Network`: id, name, `datastream` rel,
  `commit` rel), `network_traveltime.py`; `datastream.py` has `network_id` FK,
  `network` relationship and a `Network@iot.navigationLink` column;
  `commit.py` cross-links Network when auth is on.
- Endpoints: `api/app/v1/endpoints/{create,read,update,delete}/network.py`.
- Activation (`api/app/v1/api.py`): `if NETWORK:` adds the `Networks` entry to the
  service root and registers the 4 network routers. With `NETWORK=0` none of that
  is registered and the schema objects do not exist.

**Interaction with the 8 STA entities:** only **Datastream** (the `network_id`
FK). When ON, `network_id` is **NOT NULL** on Datastream — i.e. *every* Datastream
must belong to a Network. The other 7 entities are untouched directly.

---

## 2. AUTHORIZATION (flag `AUTHORIZATION`, currently 0)

**Purpose (confirmed + refined):** users are **real PostgreSQL roles** with
row-level security; additionally every STA table gets a `commit_id` FK to a
`Commit` table for chi-ha-fatto-cosa audit. ✅ accurate.

**Where it lives**
- Schema (`database/istsos_auth.sql`, all gated `IF current_setting('custom.authorization')`):
  `SET ROLE "administrator"`; creates `User` and **`Commit`** tables; **adds
  `commit_id` to all 8 STA tables** (`NOT NULL` FK to `Commit` with ON DELETE
  CASCADE — Datastream's is nullable); per-table SQL function
  **`Commit@iot.navigationLink(<Entity>)`** → `/<Entities>(id)/Commit(commit_id)`,
  plus reverse `…@iot.navigationLink(Commit)`; (if `custom.network` also wires
  Network↔Commit). Roles: `administrator WITH CREATEROLE`; users are created as
  PG roles `IN ROLE administrator`. RLS / per-role policies are part of this
  authorization layer (the user/policy model + `SET ROLE` per request).
- Code: `api/app/oauth.py` (JWT, `OAuth2PasswordBearer(tokenUrl="Login")`,
  `get_current_user`, `create_refresh_token`), `api/app/rbac_roles.py`,
  `api/app/v1/endpoints/create/login.py` (**`/Login`, `/Refresh`, `/Logout`**),
  the `user.py`/`policy.py` CRUD endpoints, and the `set_role`/`set_commit` helpers
  the write endpoints call when `current_user` is present.
- Activation (`api.py`): `if AUTHORIZATION:` registers login + user + policy
  routers and the auth wiring. Write endpoints add a mandatory `commit-message`
  header when `VERSIONING or AUTHORIZATION`. With `AUTHORIZATION=0`: no Commit
  table, no `commit_id`, no RLS, no `SET ROLE`, login/user/policy routes absent.

**Interaction with the 8 STA entities:** **all 8** gain `commit_id` + a
`Commit@iot.navigationLink`; RLS filters their rows per role; every write records
a `Commit` row.

---

## 3. VERSIONING (flag `VERSIONING`, currently 0)

**Purpose (confirmed + refined):** system-time history of every STA entity's state
and of operations over time, tied to the commit. ✅ accurate; the mechanism is
trigger-based system-versioning.

**Where it lives**
- Schema (`database/istsos_schema_versioning.sql`, `custom.versioning`): a
  SYSTEM_TIME extension. Adds a `systemTimeValidity tstzrange` column to STA
  tables; creates a parallel `sensorthings_history.<table>` for each; a
  `BEFORE INSERT/UPDATE/DELETE` trigger (`istsos_mutate_history`) that closes the
  old row's validity range and copies it into the history table; a trigger
  (`istsos_prevent_table_update`) that makes history immutable; and `*_traveltime`
  views to query modification history. History rows reference `commit_id`.
- Models: the `*_traveltime.py` models (`datastream_traveltime.py`, etc.) +
  `commit.py`; `read/commit.py` endpoint.
- Activation (`api.py`): `if VERSIONING:` registers the `read_commit` router and
  enables the `commit-message` header on writes (shared with auth via `set_commit`).
- **Dependency:** versioning history is keyed on `commit_id`, which is created by
  the **authorization** schema. So versioning realistically needs the commit
  infrastructure (i.e. likely `AUTHORIZATION=1` too) — to be confirmed when enabling.

**Interaction with the 8 STA entities:** **all 8** get `systemTimeValidity` +
`_history` tables + `_traveltime` views; every write fires the history trigger.

---

## 4. CRITICAL POINT (b) — do extension fields appear in STA responses?

**Current config (all flags OFF): NO.** A full `GET /Datastreams?$top=1` body
returns only standard 18-088 fields:
```
@iot.id, @iot.selfLink, Thing@iot.navigationLink, Sensor@iot.navigationLink,
ObservedProperty@iot.navigationLink, Observations@iot.navigationLink, name,
description, unitOfMeasurement, observationType, observedArea, phenomenonTime,
resultTime, properties
```
No `network_id`, no `Network@iot.navigationLink`, no `commit_id`, no
`Commit@iot.navigationLink`, no `systemTimeValidity`. Things/Datastreams/
Observations all checked — same result. This is consistent with the suite being
green.

**But this is trivially true because the columns/tables don't exist with the flags
OFF — it does NOT prove the ON behavior.** The schema deliberately defines
`Network@iot.navigationLink(Datastream)`, `Commit@iot.navigationLink(<Entity>)`
and the `systemTimeValidity` column, which are **designed to surface** when active.
Whether they then appear in the standard STA response (an **extra field outside
18-088 → a conformance concern**) is:

> **NOT VERIFIABLE with the flags OFF.** To check each: enable the flag in `.env`
> (`NETWORK=1` / `AUTHORIZATION=1` / `VERSIONING=1`), **rebuild the DB** (the schema
> DDL is gated) and **restart** the service, then re-inspect a GET body. I have
> **not** done this — it needs your confirmation.

This is the single most important conformance question for the extensions, and it
can only be answered with the relevant flag ON.

---

## 5. CRITICAL POINT (c) — is conformance role-dependent?

**Today (AUTHORIZATION=0): role-INDEPENDENT.** RLS is part of the gated auth layer,
so it is inactive; no `SET ROLE` is issued; all 399 queries run as the single
admin connection (`ISTSOS_ADMIN=admin`). Result sets and `$count` are therefore
stable regardless of "user".

**Under AUTHORIZATION=1: role-DEPENDENT.** Users become PostgreSQL roles, RLS is
enforced, and each request `SET ROLE`s to the authenticated user. A reduced-
privilege role would see a **different row set**, so the same `$count`/`$filter`
result-set assertions the 399 rely on could change — and the suite (which sends no
token) would either be rejected or run under whatever default applies. **Not
verifiable without enabling AUTHORIZATION.** Implication: the current green
baseline is valid *for the admin/no-RLS config only*; any auth testing needs its
own baseline.

---

## 6. Proposed tests — NETWORK (additive; proposal only, not implemented)

NETWORK is the safest to test (no auth/token machinery). **Key constraint:**
because `Datastream.network_id` is **NOT NULL** when NETWORK is ON, enabling the
flag changes Datastream creation — **the current `entitiesDefault.json` seed and
the c02 Datastream tests would fail** (a Datastream POST without a network → NOT
NULL violation). So NETWORK tests **cannot share the 399 run**; they need their own
flag config and their own dataset.

- **Config:** a separate run with `NETWORK=1` (DB rebuilt, service restarted). Keep
  AUTHORIZATION/VERSIONING off to isolate the variable.
- **New class:** `tests/conformance/network/` with a dedicated `@pytest.mark.network`
  marker (registered in `pytest.ini`), inheriting the root `conftest.py` fixtures.
- **Dataset:** a `network/conftest.py` `network_seed` fixture that POSTs a
  `Network`, then a Thing → Datastream(**with** that network) → Sensor/OP/Obs
  subtree; teardown deletes it (Network ON DELETE CASCADE removes its datastreams).
  The standard `seed` is unusable here (no network).
- **Tests to cover:**
  1. `POST /Networks` (201 + Location), `GET /Networks`, `GET /Networks(id)`.
  2. Datastream→Network nav: `GET /Datastreams(id)/Network`.
  3. Network→Datastreams nav: `GET /Networks(id)/Datastreams` (returns the linked set).
  4. `$expand=Network` on Datastream (if supported).
  5. **Conformance check (b) for network:** assert whether `Network@iot.navigationLink`
     appears in the standard `GET /Datastreams` body — document it as an extra field
     vs 18-088 (this is the value of the test: it pins the ON behavior).
  6. Datastream POST **without** a network under NETWORK=1 → expected error
     (NOT NULL) — pins the requirement.
  7. Cascade: deleting a Network removes its Datastreams (and cascades down).

---

## 7. Auth & Versioning — what testing would need + risks (no proposal yet)

### AUTHORIZATION
- **Needs:** `AUTHORIZATION=1` + `SECRET_KEY` + DB rebuilt with the auth schema
  (Commit, commit_id, RLS, roles); test PG roles/users created and torn down; a
  login flow (`POST /Login` → JWT, `/Refresh`, `/Logout`) and token-bearing
  requests; per-role request fixtures.
- **Tests would target:** login/refresh/logout; RLS isolation (role A cannot see
  role B's rows); commit audit (each write creates a Commit; `commit_id` /
  `Commit@iot.navigationLink` correctness); the **conformance (b)** question for
  `commit_id`/`Commit@iot.navigationLink`.
- **Risks:** managing real PG roles/users lifecycle in tests; secret/token
  handling; **RLS makes the existing 399 role-dependent** (they'd need an admin/
  bypass role or a re-baseline under auth); `commit_id` NOT NULL ⇒ every write must
  carry a commit (and a `commit-message` header); test isolation across roles.

### VERSIONING
- **Needs:** `VERSIONING=1` (almost certainly **with** AUTHORIZATION for the
  commit/`commit_id` infra) + DB rebuilt with the system_time DDL (history schema,
  triggers, `_traveltime` views, `systemTimeValidity`).
- **Tests would target:** mutate an entity → assert a history row with a closed
  `systemTimeValidity` range; history immutability (UPDATE/DELETE on `_history`
  rejected); `_traveltime`/as-of queries; the **conformance (b)** question for
  `systemTimeValidity` leaking into STA responses.
- **Risks:** dependency on auth/commit; the history trigger fires on **every**
  write (behavioral/perf impact on the whole suite); history growth/cleanup;
  isolating history between tests; possible `systemTimeValidity` field leak.

---

## 8. Open question for you

To answer point (b) for any extension I must enable its flag, **rebuild the DB and
restart the service** (the schema is gated). Per your constraint I have not touched
`.env`. **Do you want me to enable one flag (which one?) so I can verify whether the
extension fields appear in STA responses** — or keep everything OFF and treat the
ON-behavior as a documented unknown for now?
