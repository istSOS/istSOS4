import os
import sys
from pathlib import Path


# Ensure api/ is on sys.path so "app" resolves to api/app.
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Supply the minimum application configuration before importing app modules.
os.environ.setdefault("DEBUG", "0")
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")

from app import VERSION  # noqa: E402
from app.sta2rest.sta2rest import STA2REST  # noqa: E402


INSTANT = "2026-06-10T05:00:00Z"


def compile_filter(entity: str, expression: str) -> str:
    result = STA2REST.convert_query(
        f"{VERSION}/{entity}?$filter={expression}"
    )
    return result["main_query"]


def test_observation_eq_checks_that_interval_contains_instant():
    sql = compile_filter("Observations", f"phenomenonTime eq {INSTANT}")

    assert '"Observation"."phenomenonTimeStart" <=' in sql
    assert '"Observation"."phenomenonTimeEnd" >=' in sql


def test_datastream_eq_checks_that_interval_contains_instant():
    sql = compile_filter("Datastreams", f"phenomenonTime eq {INSTANT}")

    assert 'lower(sensorthings."Datastream"."phenomenonTime") <=' in sql
    assert 'upper(sensorthings."Datastream"."phenomenonTime") >=' in sql


def test_observation_ordering_uses_the_correct_interval_bound():
    after_sql = compile_filter(
        "Observations", f"phenomenonTime ge {INSTANT}"
    )
    before_sql = compile_filter(
        "Observations", f"phenomenonTime le {INSTANT}"
    )

    assert '"Observation"."phenomenonTimeStart" >=' in after_sql
    assert '"Observation"."phenomenonTimeEnd" <=' in before_sql


def test_datastream_ordering_uses_the_correct_interval_bound():
    after_sql = compile_filter(
        "Datastreams", f"phenomenonTime gt {INSTANT}"
    )
    before_sql = compile_filter(
        "Datastreams", f"phenomenonTime lt {INSTANT}"
    )

    assert 'lower(sensorthings."Datastream"."phenomenonTime") >' in after_sql
    assert 'upper(sensorthings."Datastream"."phenomenonTime") <' in before_sql


def test_phenomenon_time_ne_is_the_negation_of_containment():
    observation_sql = compile_filter(
        "Observations", f"phenomenonTime ne {INSTANT}"
    )
    datastream_sql = compile_filter(
        "Datastreams", f"phenomenonTime ne {INSTANT}"
    )

    assert "NOT (" in observation_sql
    assert '"Observation"."phenomenonTimeStart" <=' in observation_sql
    assert '"Observation"."phenomenonTimeEnd" >=' in observation_sql
    assert "NOT (" in datastream_sql
    assert 'lower(sensorthings."Datastream"."phenomenonTime") <=' in datastream_sql
    assert 'upper(sensorthings."Datastream"."phenomenonTime") >=' in datastream_sql


def test_reversed_comparison_reverses_interval_direction():
    sql = compile_filter("Observations", f"{INSTANT} gt phenomenonTime")

    assert '"Observation"."phenomenonTimeEnd" <' in sql


def test_null_comparison_is_not_treated_as_an_instant():
    datastream_sql = compile_filter(
        "Datastreams", "phenomenonTime eq null"
    )
    observation_sql = compile_filter(
        "Observations", "phenomenonTime ne null"
    )

    assert '"Datastream"."phenomenonTime" IS NULL' in datastream_sql
    assert '"Observation"."phenomenonTimeStart" IS NOT NULL' in observation_sql


def test_explicit_lower_and_upper_keep_using_the_same_bound_resolver():
    observation_lower_sql = compile_filter(
        "Observations", f"lower(phenomenonTime) eq {INSTANT}"
    )
    observation_upper_sql = compile_filter(
        "Observations", f"upper(phenomenonTime) eq {INSTANT}"
    )
    datastream_lower_sql = compile_filter(
        "Datastreams", f"lower(phenomenonTime) eq {INSTANT}"
    )
    datastream_upper_sql = compile_filter(
        "Datastreams", f"upper(phenomenonTime) eq {INSTANT}"
    )

    assert '"Observation"."phenomenonTimeStart" =' in observation_lower_sql
    assert '"Observation"."phenomenonTimeEnd" =' in observation_upper_sql
    assert 'lower(sensorthings."Datastream"."phenomenonTime") =' in (
        datastream_lower_sql
    )
    assert 'upper(sensorthings."Datastream"."phenomenonTime") =' in (
        datastream_upper_sql
    )
