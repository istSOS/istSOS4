# NETWORK vs conformance coverage gap (Sensor / ObservedProperty parity)

Goal: give the **Network** entity the same test depth that the conformance suite
gives a *parent* of Datastream (Sensor / ObservedProperty) — **only** for Network
and its Datastream relation. The 8 standard entities are NOT re-tested (they don't
change with NETWORK).

- **Scope of comparison:** the Sensor/ObservedProperty cases in
  `tests/conformance/{c01,c02,c03}/` (Sensor and ObservedProperty are symmetric —
  one row covers both).
- **Network suite:** `tests/extensions/network/` (**29 tests**: 27 passed, 2 xfailed), requires `NETWORK=1`.
- **STATUS: Plan A applied.** Every gap below that uses an existing route now has a
  test; the two route deviations (PUT, nav-link-POST) are documented as `xfail`. No
  routes were added (source untouched). The ❌/⚠️ marks in the tables are the
  original snapshot — see **§Resolution (Plan A)** for the test now covering each.

Legend: ✅ covered · ❌ MISSING (no test) · ⚠️ DEVIATION (Network differs from the
standard; real state shown — would be `xfail` until/unless implemented) · ➕ Network
already exceeds the Sensor/ObsProp baseline.

## Network routes that actually exist (verified)
`POST /Networks` · `GET /Networks` · `GET /Networks(id)` · `PATCH /Networks(id)` ·
`DELETE /Networks(id)`. **No `PUT /Networks(id)`** (→405). **No
`POST /Networks(id)/Datastreams`** (→405).

---

## A. Read (c01)

| # | Conformance case (Sensor/ObsProp) | conformance test | Network equivalent | Status |
|---|---|---|---|---|
| 1 | Collection GET → 200 + value[] | `test_collection_returns_200` (generic) | `test_networks_collection_returns_200` | ✅ |
| 2 | Entity by id | `test_sensor_by_id` / `test_observed_property_by_id` | `test_network_entity_by_id` | ✅ |
| 3 | Control info `@iot.id` / `@iot.selfLink` | `test_sensor_has_iot_id_and_self_link` | `test_network_control_info` | ✅ |
| 4 | Navigation-link advertised on entity | `test_sensor_navigation_links` | `test_network_control_info` (asserts `Datastreams@iot.navigationLink`) | ✅ |
| 5 | selfLink resolves to same entity | (implicit) | `test_network_selflink_resolves` | ➕ |
| 6 | Mandatory-properties check | `test_sensor_mandatory_properties` | only `name` exists; presence checked in `test_network_control_info`, but **no dedicated mandatory/negative test** | ❌ |
| 7 | Property access `/Sensors(id)/<prop>` | `test_sensor_metadata_property`, `test_ds1_sensor_name` | `/Networks(id)/name` | ❌ MISSING |
| 8 | Property `$value` `/…/<prop>/$value` | `test_sensor_name_dollar_value` | `/Networks(id)/name/$value` | ❌ MISSING |
| 9 | Property response has exactly one key | `test_sensor_metadata_response_has_one_key` | `test_network_select_name` (analogous via `$select`) | ✅ |
| 10 | Datamodel relations (both directions) | `test_sensor_relations` | covered by navigation rows (B) | ✅ |

## B. Navigation (c01)

| # | Conformance case | conformance test | Network equivalent | Status |
|---|---|---|---|---|
| 11 | Many-to-one: `Datastreams(id)/Sensor` | `test_ds1_to_sensor` / `test_ds2_to_sensor` | `test_datastream_to_network` | ✅ |
| 12 | One-to-many: `Sensors(id)/Datastreams` | `test_sensor_ds1_to_datastreams` | `test_network_to_datastreams_grouping_A` / `_B` | ✅ |
| 13 | `$ref` many-to-one (`Datastreams(id)/Sensor/$ref`) | `test_ds1_sensor_ref_is_single` | `test_datastream_network_ref` | ✅ |
| 14 | `$ref` one-to-many (`Sensors(id)/Datastreams/$ref`) | *(none for Sensor)* | `test_network_datastreams_ref` | ➕ |
| 15 | Roundtrip both directions | `test_ds1_sensor_roundtrip` | rows 11 + 12 | ✅ |
| 16 | Datastream advertises the parent nav-link | `test_datastream_navigation_links` (generic) | `test_datastream_exposes_network_navlink` | ✅ |

## C. Create / Update / Delete (c02)

| # | Conformance case | conformance test | Network equivalent | Status |
|---|---|---|---|---|
| 17 | POST entity | `test_post_sensor` / `test_post_observed_property` | `test_post_network_minimal` | ✅ |
| 18 | Deep insert (parent inline) | `test_deep_insert` | `test_deep_insert_thing_datastream_with_network_inline` + `test_deep_insert_network_with_datastreams` | ➕ |
| 19 | Link-to-existing via `{"@iot.id"}` | `test_create_with_existing_link` | `test_direct_post_datastream_with_network_link` | ✅ |
| 20 | Datastream requires the parent (mandatory rel) | *(implicit for Sensor)* | `test_datastream_requires_network` | ➕ |
| 21 | POST to navigation link (create child under parent) | `test_post_to_navigation_link_*` (Thing/Datastream; **not** Sensor) | `POST /Networks(id)/Datastreams` → **405** (no route) | ⚠️ DEVIATION |
| 22 | PATCH entity | `test_patch_sensor` / `test_patch_observed_property` | `PATCH /Networks(id)` route **exists**, **untested** | ❌ MISSING |
| 23 | PATCH relation relink | `test_patch_relation_relink_sensor` | relink `Datastream.Network` via PATCH (now allowed after the gating fix), **untested** | ❌ MISSING |
| 24 | PUT entity | `test_put_missing_mandatory_property_sensor` (+ `test_put_replace_thing`) | **no `PUT /Networks(id)`** → 405 | ⚠️ DEVIATION |
| 25 | Validation: missing mandatory property | `test_validation_missing_*`, `test_put_missing_mandatory_property_sensor` | `POST /Networks` without `name` → (unverified, no test) | ❌ MISSING |
| 26 | Validation: entity-specific (Sensor string metadata) | `test_validation_sensor_string_metadata` | N/A — Network has only `name` | — |
| 27 | DELETE entity (+ 404 after) | `test_delete_sensor` / `test_delete_observed_property` | `DELETE /Networks(id)` route **exists** (used in teardown), **no dedicated test** | ❌ MISSING |

## D. Filtering / query options (c03)

| # | Conformance case | conformance test | Network equivalent | Status |
|---|---|---|---|---|
| 28 | `$expand=<parent>` on Datastreams | `test_expand_single` / `test_expand_nested_path` | `test_expand_network_on_datastream` | ✅ |
| 29 | `$filter=<parent>/name eq …` (relation filter) | `test_relation_sensor_name`, `test_relation_observedproperty_two_levels` | `test_filter_datastreams_by_network_name` | ✅ |
| 30 | `$select` scalar property | `test_select_single_property` / `test_select_multiple_properties` | `test_network_select_name` | ✅ |
| 31 | `$select` navigation property | `test_select_navigation_property` | `$select=Network` on Datastreams | ❌ MISSING |

---

## Resolution (Plan A)

**✅ Gaps closed — each now has a test (existing routes; no source change):**

| # | Gap | New test |
|---|---|---|
| 6 | mandatory-properties (`name`) | `test_network_read.py::test_network_mandatory_properties` |
| 7 | property access `/Networks(id)/name` | `test_network_read.py::test_network_name_property` |
| 8 | property `$value` | `test_network_read.py::test_network_name_dollar_value` |
| 22 | `PATCH /Networks(id)` | `test_network_update_delete.py::test_patch_network` |
| 23 | PATCH relink `Datastream.Network` | `test_network_update_delete.py::test_patch_datastream_relink_network` |
| 25 | POST `/Networks` missing `name` → 400 | `test_network_create.py::test_post_network_missing_name_returns_400` |
| 27 | `DELETE /Networks(id)` + 404-after | `test_network_update_delete.py::test_delete_network_then_404` |
| 31 | `$select=Network` navigation | `test_network_navigation.py::test_select_network_navigation_on_datastream` |

**⚠️ DEVIATIONS documented as `xfail` (route not implemented → 405; separate feature
decision, source untouched — will `xpass` if the route is added):**

| # | Deviation | xfail test |
|---|---|---|
| 21 | `POST /Networks(id)/Datastreams` → 405 (no nav-link-POST; conformance doesn't test this for Sensor either) | `test_network_update_delete.py::test_post_to_network_datastreams_navlink` |
| 24 | `PUT /Networks(id)` → 405 (PATCH-only; PUT exists only for the 8 standard entities) | `test_network_update_delete.py::test_put_replace_network` |

**➕ Where Network already EXCEEDS the Sensor/ObsProp baseline:** one-to-many `$ref`
(#14), explicit mandatory-relation test (#20), deep-insert from both Thing and
Network sides (#18), selfLink-resolves (#5).

**Net:** Network now has full read / navigation / `$expand` / `$filter` / `$select` /
PATCH / DELETE / validation parity with Sensor/ObservedProperty (and extras). The
only non-covered behaviors are the two genuine route deviations (PUT, nav-link-POST),
held as `xfail` pending a separate feature decision.
