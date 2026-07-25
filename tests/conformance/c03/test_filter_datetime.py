"""
test_c03_filter_datetime.py -- c03 Filtering Extension: $filter built-in
DATE/TIME functions (OGC 18-088 Table 23, req/request-data/built-in-query-functions).

built-in-query-functions (Req 31) is NOT advertised by istSOS4, but the date/time
functions WORK and are asserted positively.  The seed Thing's 4 Observations carry
phenomenonTime 2015-03-03..06 (all year 2015, month 3, hour/min/sec 0, UTC), and
day() maps 1:1 to result (2015-03-0D <-> result D), giving discriminating cases.

Scoped to the seed Thing via Datastream/Thing/@iot.id (deterministic under
parallel seeds).  Per the lead: time() is exercised; if its result-set were wrong
it would be xfailed (not routed to api-fixer) -- it is correct here.
"""

from __future__ import annotations

from datetime import datetime

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


def parse_t(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def thing_scope(seed) -> str:
    return f"Datastream/Thing/@iot.id eq {format_id(seed.thing_id)}"


def time_filter(client, seed, pred, order="result asc") -> list:
    doc = fetch(client, "Observations",
                {"$filter": f"{thing_scope(seed)} and ({pred})", "$orderby": order})
    return nums(o["result"] for o in values(doc))


def expect_time(seed, py_pred) -> list:
    pairs = sorted(zip(seed.all_results, seed.all_phenomenon_times), key=lambda x: float(x[0]))
    return [float(r) for r, t in pairs if py_pred(parse_t(t))]


def all_results(seed) -> list:
    return sorted(nums(seed.all_results))


# ===========================================================================
# Component-extraction functions
# ===========================================================================
def test_year(client, seed):
    """req/request-data/built-in-query-functions -- year(): all four are 2015."""
    assert time_filter(client, seed, "year(phenomenonTime) eq 2015") == all_results(seed)


def test_year_negative(client, seed):
    """req/request-data/built-in-query-functions -- year() non-match -> empty."""
    assert time_filter(client, seed, "year(phenomenonTime) eq 1999") == []


def test_month(client, seed):
    """req/request-data/built-in-query-functions -- month(): all four are March."""
    assert time_filter(client, seed, "month(phenomenonTime) eq 3") == all_results(seed)


def test_day_discriminates(client, seed):
    """req/request-data/built-in-query-functions -- day(): 2015-03-0D maps to result D."""
    assert time_filter(client, seed, "day(phenomenonTime) eq 3") == \
        expect_time(seed, lambda d: d.day == 3)
    assert time_filter(client, seed, "day(phenomenonTime) ge 5") == \
        expect_time(seed, lambda d: d.day >= 5)


def test_hour(client, seed):
    """req/request-data/built-in-query-functions -- hour(): all four are 00h."""
    assert time_filter(client, seed, "hour(phenomenonTime) eq 0") == all_results(seed)


def test_minute(client, seed):
    """req/request-data/built-in-query-functions -- minute(): all four are :00."""
    assert time_filter(client, seed, "minute(phenomenonTime) eq 0") == all_results(seed)


def test_second(client, seed):
    """req/request-data/built-in-query-functions -- second(): all four are :00."""
    assert time_filter(client, seed, "second(phenomenonTime) eq 0") == all_results(seed)


def test_fractionalseconds(client, seed):
    """req/request-data/built-in-query-functions -- fractionalseconds(): all 0."""
    assert time_filter(client, seed, "fractionalseconds(phenomenonTime) eq 0") == all_results(seed)


def test_totaloffsetminutes(client, seed):
    """req/request-data/built-in-query-functions -- totaloffsetminutes(): seed times are UTC."""
    assert time_filter(client, seed, "totaloffsetminutes(phenomenonTime) eq 0") == all_results(seed)


def test_date_function(client, seed):
    """req/request-data/built-in-query-functions -- date() (Table 23 compares date() to
    date()); the self-comparison holds for all rows."""
    assert time_filter(client, seed, "date(phenomenonTime) eq date(phenomenonTime)") == all_results(seed)


def test_time_function(client, seed):
    """req/request-data/built-in-query-functions -- time() (Table 23); self-comparison
    holds for all rows.  (Unadvertised function -- would be xfailed if wrong; it is correct.)"""
    assert time_filter(client, seed, "time(phenomenonTime) eq time(phenomenonTime)") == all_results(seed)


# ===========================================================================
# Reference-time functions  (now / mindatetime / maxdatetime)
# ===========================================================================
def test_now(client, seed):
    """req/request-data/built-in-query-functions -- now(): all seed (2015) precede now."""
    assert time_filter(client, seed, "phenomenonTime lt now()") == all_results(seed)


def test_now_negative(client, seed):
    """req/request-data/built-in-query-functions -- nothing is dated after now()."""
    assert time_filter(client, seed, "phenomenonTime gt now()") == []


def test_mindatetime(client, seed):
    """req/request-data/built-in-query-functions -- mindatetime(): all seed times exceed it."""
    assert time_filter(client, seed, "phenomenonTime gt mindatetime()") == all_results(seed)


def test_maxdatetime(client, seed):
    """req/request-data/built-in-query-functions -- maxdatetime(): all seed times precede it."""
    assert time_filter(client, seed, "phenomenonTime lt maxdatetime()") == all_results(seed)


# ===========================================================================
# Instant / interval comparisons on phenomenonTime
# ===========================================================================
def test_instant_gt_literal(client, seed):
    """req/request-data/filter -- gt against a datetime instant literal -> {5,6}."""
    cut = "2015-03-04T12:00:00Z"
    assert time_filter(client, seed, f"phenomenonTime gt {cut}") == \
        expect_time(seed, lambda d: d > parse_t(cut))


def test_closed_interval(client, seed):
    """req/request-data/filter -- closed interval (ge .. and le ..) on phenomenonTime."""
    lo, hi = "2015-03-04T00:00:00Z", "2015-03-05T00:00:00Z"
    assert time_filter(client, seed, f"phenomenonTime ge {lo} and phenomenonTime le {hi}") == \
        expect_time(seed, lambda d: parse_t(lo) <= d <= parse_t(hi))
