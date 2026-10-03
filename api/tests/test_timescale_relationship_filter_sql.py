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


TIME_FILTER = (
    "phenomenonTime ge '2026-06-10T05:00:00Z' and "
    "phenomenonTime lt '2026-06-10T06:00:00Z'"
)


def compile_observation_query(relationship_filter: str) -> str:
    result = STA2REST.convert_query(
        f"{VERSION}/Observations?"
        f"$filter={relationship_filter} and {TIME_FILTER}"
        "&$orderby=phenomenonTime asc,id asc&$top=2000"
    )
    return result["main_query"]


def test_many_to_one_datastream_id_filter_stays_on_outer_query():
    sql = compile_observation_query("Datastream/id eq 19")

    assert 'JOIN sensorthings."Datastream"' in sql
    assert 'sensorthings."Observation"."phenomenonTimeStart" >=' in sql
    assert " IN (SELECT" not in sql


def test_many_to_one_datastream_name_filter_stays_on_outer_query():
    sql = compile_observation_query("Datastream/name eq 'P_PON'")

    assert 'JOIN sensorthings."Datastream"' in sql
    assert 'sensorthings."Observation"."phenomenonTimeStart" >=' in sql
    assert " IN (SELECT" not in sql


def test_one_to_many_relationship_filter_keeps_duplicate_safe_semi_join():
    result = STA2REST.convert_query(
        f"{VERSION}/Things?$filter=Datastreams/id eq 19"
    )

    assert " IN (SELECT" in result["main_query"]
