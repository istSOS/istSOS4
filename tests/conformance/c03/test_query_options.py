"""
test_query_options.py -- OGC SensorThings API v1.1 Filtering Extension (c03):
SYSTEM QUERY OPTIONS + combinations + edge cases.

Conformance class ``conf/request-data`` (OGC 18-088, Annex A.2.1): $orderby,
$top, $skip, $count, @iot.nextLink (server-driven paging), $select, $expand.
Req ids under ``req/request-data/...``.

The companion files cover $filter:
  test_c03_filter_logic_arith.py  -- comparison / logical / arithmetic + relations
  test_c03_filter_string.py       -- string functions
  test_c03_filter_datetime.py     -- date/time functions
  test_c03_filter_geo.py          -- geospatial / spatial-relationship functions

SEED (read-only entitiesDefault.json subtree; see sample_data.py):
  Thing "thing name 1"
   +- Location "location name 1"  Point(-117.05, 51.05)
   +- DS1 "datastream name 1" (Luminous Flux) -> results [3,4] @ 2015-03-03/04
   +- DS2 "datastream name 2" (Tempretaure)   -> results [5,6] @ 2015-03-05/06
  All four Observations: results [3,4,5,6], times 2015-03-03..06.

The local DB is NOT empty and parallel xdist workers each seed an identically
named subtree, so EVERY result-set assertion is scoped by a seed *id*:
  * per datastream:  GET /Datastreams(<id>)/Observations
  * across the Thing: $filter=Datastream/Thing/@iot.id eq <seed.thing_id> and ...
Order-invariant tests on shared collections assert only the ordering invariant.
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

import pytest

from client import format_id

pytestmark = pytest.mark.c03


# --------------------------------------------------------------------------
# shared helpers (kept self-contained per file to respect single-file ownership)
# --------------------------------------------------------------------------
def fetch(client, path, params=None) -> dict:
    r = client.get(path, params=params)
    assert r.status_code == 200, (
        f"GET {path} params={params} -> {r.status_code}: {r.text[:400]}"
    )
    return r.json()


def values(doc) -> list:
    return doc.get("value", [])


def ids_of(doc) -> list:
    return [o["@iot.id"] for o in values(doc)]


def nums(xs) -> list:
    return [float(x) for x in xs]


def results_of(doc) -> list:
    return nums(o["result"] for o in values(doc))


def norm_t(s: str) -> str:
    return s.replace("+00:00", "Z")


def parse_t(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def ptimes_of(doc) -> list:
    return [norm_t(o["phenomenonTime"]) for o in values(doc)]


def lit(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def thing_scope(seed) -> str:
    """$filter clause selecting exactly the seed Thing's 4 Observations."""
    return f"Datastream/Thing/@iot.id eq {format_id(seed.thing_id)}"


def ds_obs_path(ds) -> str:
    return f"Datastreams({format_id(ds.id)})/Observations"


def thing_obs(client, seed, pred=None, order="result asc") -> dict:
    flt = thing_scope(seed) + (f" and {pred}" if pred else "")
    params = {"$filter": flt}
    if order:
        params["$orderby"] = order
    return fetch(client, "Observations", params)


def resolve_next(client, nxt: str) -> str:
    """Resolve an @iot.nextLink to an absolute URL, accepting both forms the spec
    permits: an absolute URL (istSOS4's live form) is used verbatim; a relative
    reference (absolute-path like '/v4/v1.1/Observations?...' or a bare
    'Observations?...') is resolved against the client base URL. The already
    percent-encoded query string is preserved (passed through, never re-encoded)."""
    if nxt.startswith(("http://", "https://")):
        return nxt
    base = urlsplit(client.base_url)
    if nxt.startswith("/"):
        return urlunsplit((base.scheme, base.netloc, nxt, "", ""))
    return f"{client.base_url}/{nxt}"


def follow_pages(client, path, params):
    pages = [fetch(client, path, params)]
    vals = list(values(pages[0]))
    nxt = pages[0].get("@iot.nextLink")
    guard = 0
    while nxt and guard < 50:
        guard += 1
        page = client.nav(resolve_next(client, nxt))   # opaque nextLink (relative or absolute)
        pages.append(page)
        vals.extend(values(page))
        nxt = page.get("@iot.nextLink")
    return pages, vals


# ===========================================================================
# 0. seed scope sanity
# ===========================================================================
def test_seed_scope_thing_has_four_observations(client, seed):
    """req/request-data/filter -- the Thing-scoped filter yields exactly the 4 seeded rows."""
    doc = thing_obs(client, seed)
    assert results_of(doc) == nums(seed.all_results) == [3.0, 4.0, 5.0, 6.0]
    assert set(ids_of(doc)) == set(seed.all_observation_ids)


def test_seed_scope_per_datastream(client, seed):
    """req/request-data/filter -- each seed datastream scopes to its own 2 Observations."""
    d1 = fetch(client, ds_obs_path(seed.ds1), {"$orderby": "result asc"})
    d2 = fetch(client, ds_obs_path(seed.ds2), {"$orderby": "result asc"})
    assert results_of(d1) == nums(seed.ds1.results) == [3.0, 4.0]
    assert results_of(d2) == nums(seed.ds2.results) == [5.0, 6.0]


# ===========================================================================
# 1. $orderby  (req/request-data/orderby, req/request-data/order)
# ===========================================================================
def test_orderby_result_asc(client, seed):
    """req/request-data/orderby -- ascending numeric sort."""
    assert results_of(thing_obs(client, seed, order="result asc")) == sorted(nums(seed.all_results))


def test_orderby_result_desc(client, seed):
    """req/request-data/orderby -- descending numeric sort."""
    assert results_of(thing_obs(client, seed, order="result desc")) == sorted(nums(seed.all_results), reverse=True)


def test_orderby_default_is_ascending(client, seed):
    """req/request-data/orderby -- 'If asc or desc is not specified ... ascending order.'"""
    assert results_of(thing_obs(client, seed, order="result")) == sorted(nums(seed.all_results))


def test_orderby_phenomenontime_asc(client, seed):
    """req/request-data/orderby -- ascending temporal sort on phenomenonTime."""
    doc = thing_obs(client, seed, order="phenomenonTime asc")
    assert ptimes_of(doc) == [norm_t(t) for t in sorted(seed.all_phenomenon_times, key=parse_t)]


def test_orderby_phenomenontime_desc(client, seed):
    """req/request-data/orderby -- descending temporal sort on phenomenonTime."""
    doc = thing_obs(client, seed, order="phenomenonTime desc")
    assert ptimes_of(doc) == [norm_t(t) for t in sorted(seed.all_phenomenon_times, key=parse_t, reverse=True)]


def test_orderby_multikey_primary(client, seed):
    """req/request-data/orderby -- comma-separated multi-key list; the primary key governs."""
    doc = thing_obs(client, seed, order="result asc,phenomenonTime desc")
    assert results_of(doc) == sorted(nums(seed.all_results))


def test_orderby_multikey_lexicographic_invariant(client):
    """req/request-data/orderby -- a two-key sort yields a lexicographic ordering; the
    secondary key id is always present + homogeneous (order-invariant, not membership)."""
    doc = fetch(client, "Observations", {"$orderby": "phenomenonTime asc,id asc", "$top": 25})
    keys = [(parse_t(o["phenomenonTime"]), o["@iot.id"]) for o in values(doc)]
    assert keys == sorted(keys)


def test_orderby_string_asc_invariant(client):
    """req/request-data/orderby -- ascending sort on a string property (Datastream.name)."""
    doc = fetch(client, "Datastreams", {"$orderby": "name asc", "$top": 20})
    names = [d["name"] for d in values(doc)]
    assert names == sorted(names)


def test_orderby_string_desc_invariant(client):
    """req/request-data/orderby -- descending sort on a string property."""
    doc = fetch(client, "Datastreams", {"$orderby": "name desc", "$top": 20})
    names = [d["name"] for d in values(doc)]
    assert names == sorted(names, reverse=True)


# ===========================================================================
# 2. $top / $skip  (req/request-data/top, req/request-data/skip)
# ===========================================================================
def test_top_limits_results(client, seed):
    """req/request-data/top -- $top=n returns at most n items in order."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$orderby": "result asc", "$top": 2})
    assert results_of(doc) == sorted(nums(seed.all_results))[:2] == [3.0, 4.0]


def test_skip_offsets_results(client, seed):
    """req/request-data/skip -- $skip=n excludes the first n items (starts at n+1)."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$orderby": "result asc", "$skip": 2})
    assert results_of(doc) == sorted(nums(seed.all_results))[2:] == [5.0, 6.0]


def test_top_skip_windows_no_overlap(client, seed):
    """req/request-data/{top,skip} -- adjacent windows are contiguous, no overlap/gaps."""
    base = {"$filter": thing_scope(seed), "$orderby": "result asc", "$top": 2}
    w1 = fetch(client, "Observations", {**base, "$skip": 0})
    w2 = fetch(client, "Observations", {**base, "$skip": 2})
    assert results_of(w1) + results_of(w2) == sorted(nums(seed.all_results))
    assert set(ids_of(w1)).isdisjoint(ids_of(w2))


def test_skip_beyond_end_is_empty(client, seed):
    """req/request-data/skip -- skipping past the end yields an empty set."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$skip": seed.n_observations})
    assert values(doc) == []


# ===========================================================================
# 3. $count=true  (req/request-data/count)
# ===========================================================================
def test_count_true_independent_of_top(client, seed):
    """req/request-data/count -- @iot.count is the total, ignoring $top."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$count": "true", "$top": 2})
    assert doc["@iot.count"] == seed.n_observations == 4
    assert len(values(doc)) == 2


def test_count_true_per_datastream(client, seed):
    """req/request-data/count -- count on a navigation collection equals its size."""
    doc = fetch(client, ds_obs_path(seed.ds1), {"$count": "true", "$top": 1})
    assert doc["@iot.count"] == len(seed.ds1.results) == 2


def test_count_true_ignores_skip(client, seed):
    """req/request-data/count -- '$count ... SHALL ignore any $top, $skip ...'."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$count": "true", "$skip": 3})
    assert doc["@iot.count"] == seed.n_observations


def test_count_with_filter(client, seed):
    """req/request-data/count -- count reflects only rows matching $filter."""
    doc = fetch(client, "Observations",
                {"$filter": f"{thing_scope(seed)} and result gt 4", "$count": "true"})
    assert doc["@iot.count"] == len([r for r in seed.all_results if r > 4]) == 2


def test_count_false_omits_annotation(client, seed):
    """req/request-data/count -- $count=false hints the service SHALL not return a count."""
    doc = fetch(client, "Observations", {"$filter": thing_scope(seed), "$count": "false"})
    assert "@iot.count" not in doc


def test_count_invalid_value_is_client_error(client, seed):
    """req/request-data/count -- a non-boolean $count value is rejected as a client error.
    The clause cites 400; 18-088 does not test the exact code, so any 4xx is conformant
    (istSOS4 returns 422 from FastAPI/Pydantic validation)."""
    r = client.get("Observations", params={"$filter": thing_scope(seed), "$count": "maybe"})
    assert 400 <= r.status_code < 500, f"expected 4xx, got {r.status_code}: {r.text[:200]}"


def test_count_empty_set_returns_zero(client, seed):
    """req/request-data/count (now DECLARED) -- when $count=true and the result set is EMPTY,
    the service MUST still return the total count annotation with the value 0 (it MUST NOT
    omit @iot.count). The value array is empty and @iot.count is exactly 0."""
    doc = fetch(client, "Observations",
                {"$filter": f"{thing_scope(seed)} and result gt 1000", "$count": "true"})
    assert values(doc) == []
    assert "@iot.count" in doc, "$count=true on an empty set MUST still carry @iot.count"
    assert doc["@iot.count"] == 0


# ===========================================================================
# 4. @iot.nextLink  (req/request-data/pagination)
# ===========================================================================
def test_nextlink_present_when_results_exceed_page(client, seed):
    """req/request-data/pagination -- a partial response SHALL carry a nextLink."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$orderby": "result asc", "$top": 2})
    assert isinstance(doc.get("@iot.nextLink"), str)


def test_nextlink_paging_no_overlap_no_gaps(client, seed):
    """req/request-data/pagination -- following nextLink walks the whole set once, no
    overlap/gaps; the final page SHALL NOT contain a nextLink."""
    pages, vals = follow_pages(
        client, "Observations",
        {"$filter": thing_scope(seed), "$orderby": "result asc", "$top": 2})
    assert nums(o["result"] for o in vals) == sorted(nums(seed.all_results))
    assert len({o["@iot.id"] for o in vals}) == seed.n_observations
    assert "@iot.nextLink" not in pages[-1]


def test_nextlink_per_datastream_walk(client, seed):
    """req/request-data/pagination -- paging a navigation collection (top=1 of 2)."""
    pages, vals = follow_pages(client, ds_obs_path(seed.ds1),
                               {"$orderby": "result asc", "$top": 1})
    assert nums(o["result"] for o in vals) == nums(seed.ds1.results)
    assert "@iot.nextLink" not in pages[-1]


def test_nextlink_absent_when_all_fit(client, seed):
    """req/request-data/pagination -- a complete (non-partial) page has no nextLink."""
    doc = fetch(client, "Observations",
                {"$filter": thing_scope(seed), "$top": seed.n_observations})
    assert "@iot.nextLink" not in doc


def test_pagination_top_nextlink_walks_full_seed(client, seed):
    """req/request-data/pagination -- $top + @iot.nextLink server-driven paging walks the
    ENTIRE seed Observation set (4 rows) exactly once. With $top=1 the service returns one
    item per page plus a nextLink until the set is exhausted; following each nextLink (which
    may be relative or absolute) MUST yield: no duplicate ids, no gaps, every page <= $top
    items, and @iot.count (when present) equal to the seed total (4)."""
    top = 1
    first = fetch(client, "Observations", {
        "$filter": thing_scope(seed),
        "$top": top,
        "$count": "true",
        "$orderby": "id asc",
    })
    # First-page invariants: a full first page carries exactly $top items, the
    # total count (independent of $top), and -- since more rows remain -- a nextLink.
    assert len(values(first)) == top
    assert first.get("@iot.count") == seed.n_observations == 4
    assert isinstance(first.get("@iot.nextLink"), str)

    collected = []
    counts = []
    page = first
    guard = 0
    while True:
        page_vals = values(page)
        assert len(page_vals) <= top, f"page returned {len(page_vals)} > $top={top}"
        collected.extend(o["@iot.id"] for o in page_vals)
        if "@iot.count" in page:
            counts.append(page["@iot.count"])
        nxt = page.get("@iot.nextLink")
        if not nxt:
            break
        guard += 1
        assert guard < 50, "nextLink paging did not terminate"
        page = client.nav(resolve_next(client, nxt))

    # No duplicates across pages, and exactly the full seed set (no gaps).
    assert len(collected) == len(set(collected)), f"duplicate ids across pages: {collected}"
    assert set(collected) == set(seed.all_observation_ids)
    assert len(collected) == seed.n_observations == 4
    # Every count annotation the service emitted equals the true total.
    assert counts and all(c == seed.n_observations for c in counts)


# ===========================================================================
# 5. $select  (req/request-data/select)
# ===========================================================================
def test_select_single_property(client, seed):
    """req/request-data/select -- a single selected property is returned, others omitted."""
    doc = fetch(client, ds_obs_path(seed.ds1), {"$select": "result"})
    assert len(values(doc)) == 2
    for o in values(doc):
        assert "result" in o
        assert "phenomenonTime" not in o


def test_select_multiple_properties(client, seed):
    """req/request-data/select -- only the selected properties are returned."""
    doc = fetch(client, ds_obs_path(seed.ds1), {"$select": "result,phenomenonTime"})
    for o in values(doc):
        assert {"result", "phenomenonTime"}.issubset(o)
        assert "resultTime" not in o


def test_select_navigation_property(client, seed):
    """req/request-data/select -- 'Each selection clause SHALL be a property name
    (including navigation property names).'  istSOS4 (post api-fixer) returns the
    selected scalar plus the navigation link."""
    body = fetch(client, f"Things({format_id(seed.thing_id)})",
                 {"$select": "name,Datastreams"})
    assert body.get("name") == seed.thing_name
    assert "Datastreams@iot.navigationLink" in body
    assert "description" not in body  # unselected scalar omitted


# ===========================================================================
# 6. $expand  (req/request-data/expand)
# ===========================================================================
def test_expand_single(client, seed):
    """req/request-data/expand -- a single navigation property is inlined."""
    doc = fetch(client, f"Things({format_id(seed.thing_id)})", {"$expand": "Datastreams"})
    assert set(seed.datastream_ids) <= {e["@iot.id"] for e in doc["Datastreams"]}


def test_expand_multiple(client, seed):
    """req/request-data/expand -- a comma-separated list inlines several relations."""
    doc = fetch(client, f"Things({format_id(seed.thing_id)})",
                {"$expand": "Locations,Datastreams"})
    assert seed.location_id in [e["@iot.id"] for e in doc["Locations"]]
    assert set(seed.datastream_ids) <= {e["@iot.id"] for e in doc["Datastreams"]}


def test_expand_nested_path(client, seed):
    """req/request-data/expand -- a multi-level relationship (Datastreams/Observations)."""
    doc = fetch(client, f"Things({format_id(seed.thing_id)})",
                {"$expand": "Datastreams/Observations"})
    ds1 = next(d for d in doc["Datastreams"] if d["@iot.id"] == seed.ds1.id)
    assert {o["@iot.id"] for o in ds1["Observations"]} == set(seed.ds1.observation_ids)


def test_expand_nested_with_options(client, seed):
    """req/request-data/expand -- nested query options apply to the expanded set:
    Observations($top=1;$orderby=phenomenonTime desc;$select=result)."""
    doc = fetch(client, f"Datastreams({format_id(seed.ds1.id)})",
                {"$expand": "Observations($top=1;$orderby=phenomenonTime desc;$select=result)"})
    obs = doc["Observations"]
    assert len(obs) == 1
    latest_idx = max(range(len(seed.ds1.results)),
                     key=lambda i: parse_t(seed.ds1.phenomenon_times[i]))
    assert float(obs[0]["result"]) == float(seed.ds1.results[latest_idx])
    assert "phenomenonTime" not in obs[0]


def test_expand_nested_filter_on_expanded_set(client, seed):
    """req/request-data/expand -- a nested $filter restricts the expanded collection."""
    doc = fetch(client, f"Datastreams({format_id(seed.ds1.id)})",
                {"$expand": "Observations($filter=result gt 3;$orderby=result asc)"})
    assert nums(o["result"] for o in doc["Observations"]) == nums(r for r in seed.ds1.results if r > 3)


# ===========================================================================
# 7. Combinations of options
# ===========================================================================
def test_combination_all_options(client, seed):
    """req/request-data/order -- $filter + $orderby + $skip + $top + $select + $count
    evaluated together in the spec order."""
    doc = fetch(client, "Observations", {
        "$filter": f"{thing_scope(seed)} and result gt 3",
        "$orderby": "result desc",
        "$skip": 1,
        "$top": 2,
        "$select": "result",
        "$count": "true",
    })
    matching = sorted((r for r in seed.all_results if r > 3), reverse=True)  # [6,5,4]
    assert results_of(doc) == nums(matching[1:3])                            # skip1 top2 -> [5,4]
    assert doc["@iot.count"] == len(matching) == 3                           # count ignores skip/top
    for o in values(doc):
        assert "phenomenonTime" not in o


def test_combination_expand_with_orderby_and_top(client, seed):
    """req/request-data/{expand,orderby,top} -- options applied to an expanded set."""
    doc = fetch(client, f"Datastreams({format_id(seed.ds2.id)})",
                {"$expand": "Observations($orderby=result desc;$top=1;$select=result)"})
    assert nums(o["result"] for o in doc["Observations"]) == [float(max(seed.ds2.results))]


def test_combination_filter_orderby_select(client, seed):
    """req/request-data/order -- $filter narrows, $orderby sorts, $select projects."""
    doc = fetch(client, "Observations", {
        "$filter": f"{thing_scope(seed)} and result ge 4",
        "$orderby": "result asc",
        "$select": "result",
    })
    assert results_of(doc) == nums(r for r in seed.all_results if r >= 4)


# ===========================================================================
# 8. Edge cases (encoding, quote-escaping, empty sets, case sensitivity)
# ===========================================================================
def test_edge_empty_result_set(client, seed):
    """req/request-data/filter -- a filter matching nothing returns an empty value array."""
    doc = thing_obs(client, seed, "result gt 1000")
    assert values(doc) == []


def test_edge_single_quote_escaping(client, seed):
    """req/request-data/filter -- single quotes in a string literal are escaped by doubling;
    a non-matching literal returns empty (no parse error)."""
    needle = lit("O'Brien place")            # -> 'O''Brien place'
    r = client.get("Things", params={"$filter": f"name eq {needle}"})
    assert r.status_code == 200, r.text[:200]
    assert values(r.json()) == []


def test_edge_case_sensitivity_eq(client, seed):
    """req/request-data/filter -- eq on strings is case-sensitive (uppercased name won't match)."""
    doc = fetch(client, "Datastreams",
                {"$filter": f"Thing/@iot.id eq {format_id(seed.thing_id)} "
                            f"and name eq {lit(seed.ds1.name.upper())}"})
    assert values(doc) == []


def test_edge_encoded_spaces_roundtrip(client, seed):
    """req/request-data/filter -- a multi-space filter is percent-encoded and still parses."""
    doc = thing_obs(client, seed, "result gt 3 and result lt 6")
    assert results_of(doc) == nums(r for r in seed.all_results if 3 < r < 6)
