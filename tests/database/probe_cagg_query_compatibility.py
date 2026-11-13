#!/usr/bin/env python3
"""Check whether the existing aggregation calculation can directly be a CAGG.

Extracts the calculation CTEs from database/functions.sql, substitutes concrete
configuration values, and verifies that they work as an ordinary SELECT before
attempting CAGG creation. Uses the disposable database harness in this directory.
"""

import argparse
import json
from pathlib import Path
import re

from probe_continuous_aggregates import OFFSET, Probe, ROOT, SOURCE


def existing_calculation():
    source = (ROOT / "database/functions.sql").read_text()
    start = source.index("    WITH normal_buckets AS (")
    end = source.index("\n    upserted AS (", start)
    query = source[start:end].rstrip().removesuffix(",")
    settings = {
        "v_series_start": "TIMESTAMPTZ '2024-02-01 00:00:00+01'",
        "v_series_end": "TIMESTAMPTZ '2024-02-01 00:50:00+01'",
        "v_config_window_start": "TIMESTAMPTZ '2024-02-01 00:00:00+01'",
        "v_bucket_interval": "INTERVAL '10 minutes'",
        "v_bucket_timezone": "'Etc/GMT-1'::text",
        "v_boundary_mode": "'right_closed'::text",
        "v_available_until": "NULL::timestamptz",
        "v_availability_max_age": "INTERVAL '10 minutes'",
        "v_availability_datastream_ids": "ARRAY[2]::bigint[]",
        "v_source_datastream_id": "1::bigint",
        "v_system_time_from": "NULL::timestamptz",
        "v_system_time_to": "TIMESTAMPTZ '2026-11-05 00:00:00Z'",
        "v_empty_bucket_policy": "'zero_when_available'::text",
        "v_quality_key": "NULL::text",
        "v_quality_allowed": "NULL::jsonb",
        "v_quality_min": "NULL::double precision",
        "v_quality_max": "NULL::double precision",
        "v_expected_observation_interval": "INTERVAL '1 minute'",
        "v_min_observations": "NULL::integer",
        "v_max_consecutive_missing": "NULL::integer",
        "v_aggregation": "'SUM'::text",
        "v_conversion_factor": "1::double precision",
        "v_empty_bucket_result_quality": "1::double precision",
    }
    tokens = set(re.findall(r"\bv_\w+\b", query))
    missing = tokens - settings.keys()
    if missing:
        raise AssertionError(f"Unmapped procedure variables: {sorted(missing)}")
    query = re.sub(r"\bv_\w+\b", lambda match: settings[match[0]], query)
    return query + "\nSELECT * FROM aggregated"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="timescale/timescaledb-ha:pg18-ts2.25")
    parser.add_argument("--report", type=Path, default=Path("/tmp/istsos-cagg-query-compatibility.json"))
    args = parser.parse_args()
    probe = Probe(args.image)
    cases = []
    try:
        probe.start()
        probe.sql((ROOT / "database/functions.sql").read_text())
        probe.seed()
        calculation = existing_calculation()
        full_select = calculation + ";"
        basic = f"""SELECT datastream_id,
            time_bucket(INTERVAL '10 minutes', "phenomenonTimeStart", {OFFSET}) AS bucket,
            count(*) AS n_total, sum("resultNumber") AS sum_value
            FROM {SOURCE} GROUP BY datastream_id, 2"""
        cte = f"WITH source AS (SELECT * FROM {SOURCE}) " + basic.replace(SOURCE, "source")
        series = basic.replace(
            f"FROM {SOURCE}", f"FROM {SOURCE} CROSS JOIN generate_series(1, 1) AS g(n)"
        )
        subquery = basic.replace(
            "GROUP BY", f"WHERE EXISTS (SELECT 1 FROM {SOURCE} a WHERE a.datastream_id=2) GROUP BY"
        )
        # A table-returning wrapper also cannot hide the full calculation from
        # CAGG restrictions and still expose the underlying hypertable.
        probe.sql(f"""CREATE FUNCTION cagg_probe.existing_calculation()
            RETURNS TABLE (bucket_start timestamptz, aggregate_value double precision)
            LANGUAGE SQL STABLE AS $body$
                SELECT bucket_start, aggregate_value FROM ({calculation}) q
            $body$;""")
        wrapped = f"""SELECT time_bucket(INTERVAL '10 minutes', bucket_start, {OFFSET}) AS bucket,
            sum(aggregate_value) AS value FROM cagg_probe.existing_calculation() GROUP BY 1"""
        for name, query, expected in (
            ("simple_hypertable_grouping", basic, True),
            ("existing_calculation_ctes", full_select.removesuffix(";"), False),
            ("single_source_cte", cte, False),
            # A constant function join is accepted in 2.25.2; do not infer a
            # blanket SRF prohibition from older-version error messages.
            ("generated_series_join", series, True),
            ("availability_exists_subquery", subquery, False),
            ("table_returning_function_wrapper", wrapped, False),
        ):
            # All candidates must first be valid executable PostgreSQL queries.
            rows = int(probe.sql(f"SELECT count(*) FROM ({query}) q;"))
            result = {"name": name, "ordinary_select_rows": rows,
                      "expected_cagg_supported": expected}
            try:
                probe.sql(f"CREATE MATERIALIZED VIEW cagg_probe.{name} "
                          f"WITH (timescaledb.continuous, timescaledb.materialized_only=true) "
                          f"AS {query} WITH NO DATA;")
            except RuntimeError as error:
                result.update(cagg_supported=False, error=str(error))
            else:
                result["cagg_supported"] = True
            result["expectation_matched"] = result["cagg_supported"] == expected
            cases.append(result)
            print(json.dumps(result), flush=True)
        probe.report["compatibility"] = cases
        probe.report["summary"] = {
            "pass": sum(case["expectation_matched"] for case in cases),
            "fail": sum(not case["expectation_matched"] for case in cases),
        }
    finally:
        if probe.started:
            probe.docker("rm", "-fv", probe.container)
        args.report.write_text(json.dumps(probe.report, indent=2) + "\n")
        print(f"Report: {args.report}", flush=True)
    return int(any(not case["expectation_matched"] for case in cases))


if __name__ == "__main__":
    raise SystemExit(main())
