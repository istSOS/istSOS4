# Conservative Refactor Plan — error handling & validation uniformity

**Scope:** unify error handling / validation (coherent status codes, uniform
messages) across the **conformance (STA core) endpoints** only. No
re-architecture, no renames, no public-signature changes, no SQL/perf changes.

**Safety net (FASE 2):** after EVERY single change, run
`pytest tests/conformance -n auto -q` → must stay **394 passed**. If any test goes
red, revert that change. One small isolated diff at a time.

**STATUS: FASE 2 complete for the approved items (P1 + P2). Suite green at 394.
P3 and P4 NOT applied (not approved). See §5 for results.**

---

## 0. Off-limits modules (please confirm — your message left the paths as a placeholder)

You named the concerns (authorization, versioning, network); these are the exact
paths I found and will treat as **off-limits** (no edits; if a fix would require
touching them I stop and ask):

| Concern | Exact paths (off-limits) |
|---|---|
| authorization | `api/app/oauth.py`, `api/app/rbac_roles.py`, `api/app/v1/endpoints/create/login.py`, `api/app/v1/endpoints/{create,read,update,delete}/user.py`, `api/app/v1/endpoints/{create,read,update,delete}/policy.py` |
| network | `api/app/models/network.py`, `api/app/models/network_traveltime.py`, `api/app/v1/endpoints/{create,read,update,delete}/network.py` |
| versioning | **No dedicated module found.** Only flags `VERSIONING`/`commit-message` header + `set_commit()` woven into the write endpoints. I will NOT touch versioning logic. ⚠️ Please confirm there is no separate versioning path I should exclude. |

⚠️ **Auth woven into in-scope files:** every write endpoint contains RBAC/auth
branches — `except InsufficientPrivilegeError → 403`, and the `401/403` paths that
only fire when `AUTHORIZATION` is enabled (the conformance suite runs with it
**off**, so those paths are untested). **I will not touch any 401/403 / privilege
/ `set_role` / `set_commit` / `current_user` logic**, even inside in-scope files.

**In-scope:** the 8 STA entities across `create/ read/ update/ delete/` + the
`sta2rest` query/error layer + `utils/utils.py` validation helpers.

---

## 1. What is ALREADY uniform (no change needed)

- **Error envelope** is consistent everywhere in-scope: `{"code": N, "type":
  "error", "message": "..."}` (JSON). No stray FastAPI `{"detail": ...}` shapes,
  no raw `HTTPException`, no `422` leaks found in scope.
- **Nonexistent target entity → 404** on both read (`read.py` `StopAsyncIteration`)
  and write (PATCH/PUT/DELETE via the `not_found_message` helper / inline 404).
- **Validation key/required checks are centralized**: all 8 create endpoints use
  `require_json_content_type` / `validate_payload_keys` / `validate_required_keys`
  from `utils/utils.py` (no per-endpoint duplication of those checks).

So the envelope and the 404 case are fine. The real divergence is below.

---

## 2. Inconsistencies found (in-scope)

### INC-1 — Read maps errors to a proper ladder; writes flatten everything to 400
- **Read** (`read/read.py`): `InvalidField/InvalidCollection → 404`,
  `PostgresConnectionError/TooManyConnectionsError → 503`, unexpected → `500`
  (controlled message "Internal server error").
- **Writes** (`create/*`, `update/*`, `delete/*`, ~27 endpoints): the only
  catches are `except InsufficientPrivilegeError → 403` and a bare
  `except Exception → 400` with `message = str(e)`. Consequence: the **same
  conditions get different status codes read-vs-write** —
  - DB unavailable: read `503` vs write `400`.
  - Unexpected/internal error: read `500` vs write `400` (a server bug is
    reported to the client as "your request was bad").
  - The raw `str(e)` is leaked into `message` (non-uniform, may expose internals).
- **Files:** all in-scope `create/*.py`, `update/*.py`, `delete/*.py`.

### INC-2 — Validation helpers raise mixed exception types
- `require_json_content_type`, `validate_payload_keys`, `validate_required_keys`
  raise bare **`Exception`**; the association/value validators in the same file
  raise **`ValueError`**. All are caught by the write `except Exception → 400`, so
  status is the same today, but the inconsistency is what makes INC-1's
  "validation (400) vs internal (500)" split impossible to do cleanly.
- **File:** `api/app/utils/utils.py` (lines ~55, 342, 348).

### INC-3 — Duplicated error-handling blocks (~27×)
- The identical `except InsufficientPrivilegeError → 403` + `except Exception →
  400` JSONResponse pair is copy-pasted across ~27 write endpoints. Pure
  duplication (not a behavior bug). DRYing it touches many files at once.
- **Files:** all in-scope `create/*.py`, `update/*.py`, `delete/*.py`.

### INC-4 — Minor message-wording variance (low value)
- 404 messages differ: read `"Not Found"`, write `"<Entity> not found."`,
  resource-path `str(e)` ("Invalid field: …"). All 404, just different wording.
- **Files:** `read/read.py`, `update/*.py`.

---

## 3. Proposed changes (minimal; ranked by safety)

### P1 — Add the DB-unavailable rung to write endpoints  ✅ recommended (LOW risk)
Mirror `read.py`: insert
`except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError): → 503`
**before** the existing `except Exception → 400`, in each in-scope write endpoint.
- Coherence win: DB-down now `503` on writes too (matches reads).
- **Risk to 394:** none — the conformance suite never triggers DB-unavailable, so
  the 400 validation behavior is untouched and tests stay green.
- **Diff size:** ~4 lines/file; one file at a time, pytest after each.
- **Off-limits:** none touched.

### P2 — Make the 3 validation helpers raise `ValueError` (not bare `Exception`)  ✅ recommended (LOW risk)
In `utils/utils.py`, change `raise Exception(...)` → `raise ValueError(...)` in
`require_json_content_type`, `validate_payload_keys`, `validate_required_keys`.
- Coherence win: all validation errors raise one type; enables a future clean
  400/500 split (P3) without guesswork.
- **Risk to 394:** none — write endpoints catch `except Exception` (superclass of
  `ValueError`), so the 400 behavior and messages are unchanged.
- **Diff size:** 3 lines, 1 file.
- **Off-limits:** none.

### P3 — Separate internal errors (500) from validation (400) on writes  ⚠️ ASK (MEDIUM risk / borderline scope)
The real "coherent status" fix for INC-1: after P2, change each write endpoint's
tail to `except ValueError → 400` (validation) + `except Exception → 500`
(controlled "Internal server error"), so genuine server errors stop being
reported as 400.
- **Why I flag it:** (a) it reclassifies the catch-all, so any validation path
  that does NOT raise `ValueError`/the known types would flip to 500 and break a
  `c02` test — needs careful per-endpoint verification; (b) touching ~27 endpoints
  edges toward "re-architecture", which you ruled out. 
- **Recommendation:** do it ONLY if you approve, and then strictly one endpoint at
  a time with the 394 net. Otherwise defer.

### P4 — DRY the duplicated 403/400 blocks via a small `error_response()` helper  ⚠️ ASK (broad)
Add a tiny `error_response(status_code, message)` in `utils/utils.py` and replace
the repeated inline `JSONResponse({code,type,message})` blocks.
- **Why I flag it:** purely cosmetic de-duplication but touches ~27 files —
  against "diff piccoli e isolati". No behavior change, so 394 stays green, but
  it's a big sweep.
- **Recommendation:** defer unless you want it; if approved, do per-file batches.

### Not proposed
- INC-4 message wording: low value, message changes risk `c02` assertions → leave.
- Anything in §0 off-limits (auth/network/versioning, 401/403/privilege/RBAC).

---

## 4. What I need from you (approval gate)

1. **Confirm the off-limits paths in §0** (especially: is there a versioning path I
   missed? confirm I should leave all 401/403/privilege code alone).
2. **Approve which of P1–P4 to apply.** My recommendation: **P1 + P2 only** (safe,
   small, genuinely improve coherence). **P3** only if you accept the medium risk
   and want the 400/500 split; **P4** only if you want the de-dup sweep.

I will not touch any file until you approve. In FASE 2 I apply approved items one
at a time, reporting per change: file, synthetic diff, and the `pytest … -n auto`
result (must stay 394).

---

## 5. FASE 2 results (applied: P1 + P2; approved off-limits §0 confirmed)

Process: each diff applied in isolation, full net (`pytest tests/conformance
-n auto -q`) run after each, any file not landing at **394 passed** auto-reverted.
**Zero reverts. Final suite: 394 passed, 0 failed.**

### P2 — validators raise `ValueError` (1 file)
`api/app/utils/utils.py`: `require_json_content_type`, `validate_payload_keys`,
`validate_required_keys` now `raise ValueError` (was bare `Exception`); docstring
updated. Still caught by the write `except Exception → 400`, so status/messages
unchanged. Net: **394 passed**.

### P1 — DB-unavailable → 503 on write endpoints (27 files)
Mirrored `read.py`: inserted, before each existing `except Exception → 400`, an
`except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError) → 503`
rung (controlled message "Database temporarily unavailable"); added `import
asyncpg` where missing. Chain order per endpoint is now `403 → 503 → 400`. Applied
to the 27 in-scope write endpoints (all `create/ update/ delete/` STA-entity files
that had an endpoint try/except; `functions.py`/`json_patch.py` had none and were
skipped). Each file verified at **394 passed** before moving on; **0 reverts**.

Files (create): bulk_observation, data_array_observation, datastream,
feature_of_interest, historical_location, location, observation, observed_property,
sensor, thing. (update): datastream, feature_of_interest, historical_location,
location, observation, observed_property, put, sensor, thing. (delete): datastream,
feature_of_interest, historical_location, location, observation, observed_property,
sensor, thing.

### Not applied (as agreed)
- **P3** (400/500 split) — medium risk / borderline scope: skipped.
- **P4** (DRY the ~27 duplicated blocks) — broad sweep: skipped.
- **Off-limits** (auth incl. all 401/403/privilege/RBAC, network, versioning/commit):
  untouched.

### Coherence delta
DB-unavailable now returns **503** on writes (was 400), matching reads.
Validation errors raise a single type (`ValueError`). The write catch-all still
returns 400 for everything else (incl. genuine internal errors) — that remaining
read/write divergence is **P3**, deferred. Net unchanged at **394 passed**.

### Sanity checks
All 27 patched files `import asyncpg`; live POST/GET still 200/201; the new 503
rung sits before the 400 catch-all in each chain. (DB-unavailable itself is not
exercised by the suite, so it is not regression-covered — the change is additive
and the existing 400/403/200 paths are unchanged.)
