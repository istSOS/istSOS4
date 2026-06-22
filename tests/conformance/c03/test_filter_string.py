"""
test_c03_filter_string.py -- c03 Filtering Extension: $filter built-in STRING
functions (OGC 18-088 Table 23, req/request-data/built-in-query-functions).

NOTE on conformance class: built-in-query-functions (Req 31) IS now declared in
istSOS4's serverSettings.conformance, so every 18-088 Table 23 string function is
asserted POSITIVELY (substringof is conformant: it returns "p1 contains p0"). The
OData-4.01 `contains` alias is NOT in Table 23 (the spec's substring predicate is
substringof), so it is out of conformance scope and intentionally not tested here.

Scoped to the seed Thing's 2 Datastreams ("datastream name 1"/"...2", length 17)
via Thing/@iot.id so result-sets are deterministic under parallel seeds.
indexof is 1-based -- the OGC Table 23 example (indexof(description,'Sensor') eq 1)
is itself 1-based, so istSOS4 conforms to the OGC example (not OData's 0-based).
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


def names_of(doc) -> list:
    return [d["name"] for d in values(doc)]


def ids_of(doc) -> list:
    return [d["@iot.id"] for d in values(doc)]


def lit(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def ds_scope(seed) -> str:
    return f"Thing/@iot.id eq {format_id(seed.thing_id)}"


def string_filter(client, seed, pred):
    """Seed Datastreams (ordered by name) whose name matches the string predicate."""
    return fetch(client, "Datastreams",
                 {"$filter": f"{ds_scope(seed)} and ({pred})", "$orderby": "name asc"})


def both_names(seed):
    return sorted([seed.ds1.name, seed.ds2.name])


# ===========================================================================
# String functions that WORK
# ===========================================================================
def test_startswith(client, seed):
    """req/request-data/built-in-query-functions -- startswith: both seed datastreams."""
    doc = string_filter(client, seed, "startswith(name,'datastream')")
    assert names_of(doc) == both_names(seed)


def test_startswith_negative(client, seed):
    """req/request-data/built-in-query-functions -- startswith non-match -> empty."""
    doc = string_filter(client, seed, "startswith(name,'zzz')")
    assert values(doc) == []


def test_endswith(client, seed):
    """req/request-data/built-in-query-functions -- endswith: only 'datastream name 1'."""
    doc = string_filter(client, seed, "endswith(name,'name 1')")
    assert ids_of(doc) == [seed.ds1.id]


def test_length(client, seed):
    """req/request-data/built-in-query-functions -- length: both names are 17 chars."""
    n = len(seed.ds1.name)
    doc = string_filter(client, seed, f"length(name) eq {n}")
    assert names_of(doc) == both_names(seed)
    assert n == 17


def test_indexof(client, seed):
    """req/request-data/built-in-query-functions -- indexof (1-based per the OGC Table 23
    example).  'name' sits at 0-based index 11 in 'datastream name 1' -> 1-based 12."""
    one_based = seed.ds1.name.index("name") + 1
    doc = string_filter(client, seed, f"indexof(name,'name') eq {one_based}")
    assert names_of(doc) == both_names(seed)
    assert one_based == 12


def test_substring_two_arg(client, seed):
    """req/request-data/built-in-query-functions -- substring(p0,start,len) (0-based)."""
    doc = string_filter(client, seed, "substring(name,0,10) eq 'datastream'")
    assert names_of(doc) == both_names(seed)


def test_substring_one_arg(client, seed):
    """req/request-data/built-in-query-functions -- substring(p0,start) tail (0-based)."""
    start = seed.ds1.name.index("name")            # 0-based -> 'name 1'
    tail = seed.ds1.name[start:]
    doc = string_filter(client, seed, f"substring(name,{start}) eq {lit(tail)}")
    assert ids_of(doc) == [seed.ds1.id]


def test_tolower(client, seed):
    """req/request-data/built-in-query-functions -- tolower."""
    doc = string_filter(client, seed, f"tolower(name) eq {lit(seed.ds1.name.lower())}")
    assert ids_of(doc) == [seed.ds1.id]


def test_toupper(client, seed):
    """req/request-data/built-in-query-functions -- toupper."""
    doc = string_filter(client, seed, f"toupper(name) eq {lit(seed.ds1.name.upper())}")
    assert ids_of(doc) == [seed.ds1.id]


def test_trim(client, seed):
    """req/request-data/built-in-query-functions -- trim (seed names have no padding)."""
    doc = string_filter(client, seed, f"trim(name) eq {lit(seed.ds1.name)}")
    assert ids_of(doc) == [seed.ds1.id]


def test_concat(client, seed):
    """req/request-data/built-in-query-functions -- concat(p0,p1)."""
    doc = string_filter(client, seed, f"concat(name,'!') eq {lit(seed.ds1.name + '!')}")
    assert ids_of(doc) == [seed.ds1.id]


def test_tolower_case_insensitive_match(client, seed):
    """req/request-data/built-in-query-functions -- tolower enables a case-insensitive
    match where a plain eq (case-sensitive) would not."""
    doc = string_filter(client, seed, f"tolower(name) eq {lit(seed.ds2.name.lower())}")
    assert ids_of(doc) == [seed.ds2.id]


# ===========================================================================
# substringof -- now conformant (built-in-query-functions IS declared)
# ===========================================================================
def test_substringof(client, seed):
    """req/request-data/built-in-query-functions (now DECLARED) -- substringof(p0,p1) per
    18-088 Table 23 means "p1 contains the substring p0". Both seed datastream names
    ("datastream name 1"/"...2") contain 'datastream', so both MUST match."""
    doc = string_filter(client, seed, "substringof('datastream',name)")
    assert names_of(doc) == both_names(seed)
