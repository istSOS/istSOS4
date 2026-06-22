"""
test_c03_filter_logic_arith.py -- c03 Filtering Extension: $filter built-in
comparison, logical and arithmetic operators (OGC 18-088 Table 22,
req/request-data/built-in-filter-operations -- ADVERTISED by istSOS4) plus
$filter across relations (req/request-data/filter).

Scoped to the seed Thing's 4 Observations (results [3,4,5,6]); expectations are
derived in Python from the seed so they are self-checking.  All result-sets are
scoped by a seed id (the DB is shared and xdist workers seed identical names).
"""

from __future__ import annotations

import pytest

from client import format_id

pytestmark = pytest.mark.c03


def fetch(client, path, params=None) -> dict:
    r = client.get(path, params=params)
    assert r.status_code == 200, (
        f"GET {path} params={params} -> {r.status_code}: {r.text[:400]}"
    )
    return r.json()


def values(doc) -> list:
    return doc.get("value", [])


def nums(xs) -> list:
    return [float(x) for x in xs]


def ids_of(doc) -> list:
    return [o["@iot.id"] for o in values(doc)]


def lit(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def thing_scope(seed) -> str:
    return f"Datastream/Thing/@iot.id eq {format_id(seed.thing_id)}"


def filtered_results(client, seed, pred, order="result asc") -> list:
    """Results of the seed Thing's Observations matching `pred` (scoped + ordered)."""
    doc = fetch(client, "Observations",
                {"$filter": f"{thing_scope(seed)} and ({pred})", "$orderby": order})
    return nums(o["result"] for o in values(doc))


def expect(seed, py_pred) -> list:
    return sorted(float(r) for r in seed.all_results if py_pred(r))


# ===========================================================================
# Comparison operators  (eq ne gt ge lt le)
# ===========================================================================
def test_eq(client, seed):
    """req/request-data/built-in-filter-operations -- eq (Equal)."""
    assert filtered_results(client, seed, "result eq 4") == expect(seed, lambda r: r == 4)


def test_ne(client, seed):
    """req/request-data/built-in-filter-operations -- ne (Not equal)."""
    assert filtered_results(client, seed, "result ne 4") == expect(seed, lambda r: r != 4)


def test_gt(client, seed):
    """req/request-data/built-in-filter-operations -- gt (Greater than)."""
    assert filtered_results(client, seed, "result gt 4") == expect(seed, lambda r: r > 4)


def test_ge(client, seed):
    """req/request-data/built-in-filter-operations -- ge (Greater than or equal)."""
    assert filtered_results(client, seed, "result ge 4") == expect(seed, lambda r: r >= 4)


def test_lt(client, seed):
    """req/request-data/built-in-filter-operations -- lt (Less than)."""
    assert filtered_results(client, seed, "result lt 5") == expect(seed, lambda r: r < 5)


def test_le(client, seed):
    """req/request-data/built-in-filter-operations -- le (Less than or equal)."""
    assert filtered_results(client, seed, "result le 4") == expect(seed, lambda r: r <= 4)


def test_eq_string_property(client, seed):
    """req/request-data/built-in-filter-operations -- eq on a string property (scoped by id)."""
    doc = fetch(client, "Datastreams",
                {"$filter": f"Thing/@iot.id eq {format_id(seed.thing_id)} "
                            f"and name eq {lit(seed.ds1.name)}"})
    assert ids_of(doc) == [seed.ds1.id]


def test_eq_time_instant(client, seed):
    """req/request-data/built-in-filter-operations -- eq against a time instant literal."""
    t = seed.ds1.phenomenon_times[0]
    doc = fetch(client, f"Datastreams({format_id(seed.ds1.id)})/Observations",
                {"$filter": f"phenomenonTime eq {t}"})
    assert [o["phenomenonTime"].replace("+00:00", "Z") for o in values(doc)] == [t.replace("+00:00", "Z")]


# ===========================================================================
# Logical operators  (and or not) + parenthesised precedence
# ===========================================================================
def test_and(client, seed):
    """req/request-data/built-in-filter-operations -- logical and."""
    assert filtered_results(client, seed, "result gt 3 and result lt 6") == \
        expect(seed, lambda r: r > 3 and r < 6)


def test_or(client, seed):
    """req/request-data/built-in-filter-operations -- logical or."""
    assert filtered_results(client, seed, "result lt 4 or result gt 5") == \
        expect(seed, lambda r: r < 4 or r > 5)


def test_not(client, seed):
    """req/request-data/built-in-filter-operations -- logical negation (Table 22 'not').
    not(result gt 4) -> the complement {3,4}."""
    assert filtered_results(client, seed, "not (result gt 4)") == \
        expect(seed, lambda r: not (r > 4))


def test_not_with_function(client, seed):
    """req/request-data/built-in-filter-operations -- 'not' negating a query function."""
    # not(result ge 5) -> {3,4}
    assert filtered_results(client, seed, "not (result ge 5)") == \
        expect(seed, lambda r: not (r >= 5))


def test_precedence_parentheses(client, seed):
    """req/request-data/built-in-filter-operations -- 'and' binds tighter than 'or' and
    parentheses override it:  A or B and C  !=  (A or B) and C.
    A=result gt 5, B=result gt 3, C=result lt 5."""
    no_paren = filtered_results(client, seed, "result gt 5 or result gt 3 and result lt 5")
    paren = filtered_results(client, seed, "(result gt 5 or result gt 3) and result lt 5")
    assert no_paren == expect(seed, lambda r: r > 5 or (r > 3 and r < 5))   # {4,6}
    assert paren == expect(seed, lambda r: (r > 5 or r > 3) and r < 5)      # {4}
    assert no_paren != paren


# ===========================================================================
# Arithmetic operators inside comparisons  (add sub mul div mod)
# ===========================================================================
def test_add(client, seed):
    """req/request-data/built-in-filter-operations -- add: result add 1 eq 5 -> {4}."""
    assert filtered_results(client, seed, "result add 1 eq 5") == expect(seed, lambda r: r + 1 == 5)


def test_sub(client, seed):
    """req/request-data/built-in-filter-operations -- sub: result sub 1 eq 4 -> {5}."""
    assert filtered_results(client, seed, "result sub 1 eq 4") == expect(seed, lambda r: r - 1 == 4)


def test_mul(client, seed):
    """req/request-data/built-in-filter-operations -- mul: result mul 2 eq 8 -> {4}."""
    assert filtered_results(client, seed, "result mul 2 eq 8") == expect(seed, lambda r: r * 2 == 8)


def test_div(client, seed):
    """req/request-data/built-in-filter-operations -- div: result div 3 eq 2 -> {6}
    (verified live: 6/3=2, and 4/2=2 for div 2)."""
    assert filtered_results(client, seed, "result div 3 eq 2") == expect(seed, lambda r: r / 3 == 2)
    assert filtered_results(client, seed, "result div 2 eq 2") == expect(seed, lambda r: r / 2 == 2)


def test_mod(client, seed):
    """req/request-data/built-in-filter-operations -- mod: result mod 2 eq 0 -> {4,6}."""
    assert filtered_results(client, seed, "result mod 2 eq 0") == expect(seed, lambda r: r % 2 == 0)


# ===========================================================================
# Math functions  (round floor ceiling -- Table 23 built-in-query-functions).
# Seed results are integers, so these are the identity (mirrors FROST).
# ===========================================================================
def test_round(client, seed):
    """req/request-data/built-in-query-functions -- round(): round(4)=4 -> {4}."""
    assert filtered_results(client, seed, "round(result) eq 4") == expect(seed, lambda r: round(r) == 4)


def test_floor(client, seed):
    """req/request-data/built-in-query-functions -- floor(): floor(4)=4 -> {4}."""
    assert filtered_results(client, seed, "floor(result) eq 4") == expect(seed, lambda r: r == 4)


def test_ceiling(client, seed):
    """req/request-data/built-in-query-functions -- ceiling(): ceiling(4)=4 -> {4}."""
    assert filtered_results(client, seed, "ceiling(result) eq 4") == expect(seed, lambda r: r == 4)


# ===========================================================================
# $filter across relations  (req/request-data/filter)
# ===========================================================================
def test_relation_thing_name(client, seed):
    """req/request-data/filter -- Datastreams filtered by the related Thing's name.
    ANDed with the Thing id so the result-set is deterministic under parallel seeds."""
    doc = fetch(client, "Datastreams",
                {"$filter": f"Thing/name eq {lit(seed.thing_name)} "
                            f"and Thing/@iot.id eq {format_id(seed.thing_id)}",
                 "$orderby": "name asc"})
    assert ids_of(doc) == seed.datastream_ids


def test_relation_sensor_name(client, seed):
    """req/request-data/filter -- Datastreams filtered by the related Sensor's name."""
    doc = fetch(client, "Datastreams",
                {"$filter": f"Sensor/name eq {lit(seed.ds1.sensor_name)} "
                            f"and Thing/@iot.id eq {format_id(seed.thing_id)}"})
    assert ids_of(doc) == [seed.ds1.id]


def test_relation_observedproperty_two_levels(client, seed):
    """req/request-data/filter -- Observations filtered through a two-level relation
    Datastream/ObservedProperty/name; scoped by Thing id -> exactly DS1's 2 observations."""
    doc = fetch(client, "Observations",
                {"$filter": f"Datastream/ObservedProperty/name eq {lit(seed.ds1.observed_property_name)} "
                            f"and Datastream/Thing/@iot.id eq {format_id(seed.thing_id)}",
                 "$orderby": "result asc"})
    assert ids_of(doc) == seed.ds1.observation_ids
    assert nums(o["result"] for o in values(doc)) == nums(seed.ds1.results)


def test_relation_datastream_thing_name_two_levels(client, seed):
    """req/request-data/filter -- Observations filtered by 2-deep Datastream/Thing/name;
    scoped by Thing id -> all 4 seed observations."""
    doc = fetch(client, "Observations",
                {"$filter": f"Datastream/Thing/name eq {lit(seed.thing_name)} "
                            f"and Datastream/Thing/@iot.id eq {format_id(seed.thing_id)}",
                 "$orderby": "result asc"})
    assert set(ids_of(doc)) == set(seed.all_observation_ids)
    assert nums(o["result"] for o in values(doc)) == nums(seed.all_results)


def test_relation_by_id(client, seed):
    """req/request-data/filter -- Observations filtered by a related entity id
    (Datastream/@iot.id) -> exactly that datastream's observations."""
    doc = fetch(client, "Observations",
                {"$filter": f"Datastream/@iot.id eq {format_id(seed.ds2.id)}",
                 "$orderby": "result asc"})
    assert nums(o["result"] for o in values(doc)) == nums(seed.ds2.results)
