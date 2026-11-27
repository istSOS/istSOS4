"""
test_aggregation_jobs.py -- istSOS4 AggregationJobs regression tests (issue #262).

Verifies that renaming a source or target Datastream preserves:
  * GET /AggregationJobs(<id>) serialization (returns current id + name)
  * Duplicate detection on POST /AggregationJobs (returns 409 Conflict)
  * TimescaleDB job execution via CALL run_job(<id>) when local compose DB is reachable

Tests skip gracefully when the target API is not configured with VERSIONING=1
and DUPLICATES=0 (GET /AggregationJobs returns 503).
"""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest
import sample_data
from client import entity_id, format_id, id_from_self_link

pytestmark = pytest.mark.aggregation_jobs

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEV_COMPOSE = _REPO_ROOT / "dev_docker-compose.yml"


def _is_local_compose_target(base_url: str) -> bool:
    host = urlparse(base_url).hostname or ""
    return (
        host in {"localhost", "127.0.0.1"}
        and _DEV_COMPOSE.exists()
        and shutil.which("docker") is not None
    )


def _run_psql(sql: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(_DEV_COMPOSE),
            "exec",
            "-T",
            "database",
            "psql",
            "-U",
            "postgres",
            "-d",
            "istsos",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.aggregation_jobs
def test_aggregation_job_survives_datastream_rename(
    client, base_url, unique_name
):
    """Issue #262 — AggregationJobs keep resolving by ID after Datastream rename."""
    probe = client.get("AggregationJobs")
    if probe.status_code == 503:
        pytest.skip(
            probe.json().get(
                "message",
                "Aggregation requires VERSIONING=1 and DUPLICATES=0.",
            )
        )
    assert (
        probe.status_code == 200
    ), f"GET /AggregationJobs unexpected status {probe.status_code}: {probe.text[:300]}"

    tag = unique_name("agg262")
    headers = {"commit-message": f"test {tag}"}
    cleanup_urls: list[str] = []
    job_id = None

    try:
        t_resp = client.post(
            "Things",
            json={
                **sample_data.minimal_thing(tag),
                "Locations": [sample_data.minimal_location(tag)],
            },
            headers=headers,
        )
        assert (
            t_resp.status_code == 201
        ), f"Thing failed: {t_resp.status_code} {t_resp.text[:300]}"
        thing_url = client.location_of(t_resp)
        thing = client.nav(thing_url, params={"$expand": "Locations"})
        thing_id = entity_id(thing)
        loc_id = entity_id(thing["Locations"][0])
        cleanup_urls.append(f"{base_url}/Locations({format_id(loc_id)})")
        cleanup_urls.append(thing_url)

        s_resp = client.post(
            "Sensors", json=sample_data.minimal_sensor(tag), headers=headers
        )
        assert s_resp.status_code == 201, s_resp.text[:300]
        sensor_url = client.location_of(s_resp)
        sensor_id = id_from_self_link(sensor_url)
        cleanup_urls.append(sensor_url)

        op_resp = client.post(
            "ObservedProperties",
            json=sample_data.minimal_observed_property(tag),
            headers=headers,
        )
        assert op_resp.status_code == 201, op_resp.text[:300]
        op_url = client.location_of(op_resp)
        op_id = id_from_self_link(op_url)
        cleanup_urls.append(op_url)

        src_name = f"{tag}-src"
        tgt_name = f"{tag}-tgt"

        src_payload = sample_data.minimal_datastream(
            tag, thing_id, sensor_id, op_id
        )
        src_payload["name"] = src_name
        src_resp = client.post(
            "Datastreams", json=src_payload, headers=headers
        )
        assert src_resp.status_code == 201, src_resp.text[:300]
        src_url = client.location_of(src_resp)
        src_id = id_from_self_link(src_url)

        tgt_payload = sample_data.minimal_datastream(
            tag, thing_id, sensor_id, op_id
        )
        tgt_payload["name"] = tgt_name
        tgt_resp = client.post(
            "Datastreams", json=tgt_payload, headers=headers
        )
        assert tgt_resp.status_code == 201, tgt_resp.text[:300]
        tgt_url = client.location_of(tgt_resp)
        tgt_id = id_from_self_link(tgt_url)

        now_utc = datetime.now(timezone.utc)
        cur_hour = now_utc.replace(minute=0, second=0, microsecond=0)
        prev_hour = cur_hour - timedelta(hours=1)
        t1 = (prev_hour + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
        t2 = (prev_hour + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        t3 = (prev_hour + timedelta(minutes=45)).strftime("%Y-%m-%dT%H:%M:%SZ")
        bucket_time = (
            f"{prev_hour.strftime('%Y-%m-%dT%H:%M:%SZ')}/"
            f"{cur_hour.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )

        for ts, val in [(t1, 10.0), (t2, 20.0)]:
            obs_resp = client.post(
                "Observations",
                json={
                    "Datastream": {"@iot.id": src_id},
                    "phenomenonTime": ts,
                    "result": val,
                },
                headers=headers,
            )
            assert obs_resp.status_code == 201, obs_resp.text[:300]
            obs_id = id_from_self_link(client.location_of(obs_resp))
            foi = client.nav(
                f"Observations({format_id(obs_id)})/FeatureOfInterest"
            )
            foi_url = (
                f"{base_url}/FeaturesOfInterest({format_id(entity_id(foi))})"
            )
            if foi_url not in cleanup_urls:
                cleanup_urls.append(foi_url)

        job_resp = client.post(
            "AggregationJobs",
            json={
                "sourceDatastream": src_name,
                "targetDatastream": tgt_name,
                "bucketInterval": "1 hour",
                "aggregation": "SUM",
                "enabled": False,
            },
        )
        assert (
            job_resp.status_code == 201
        ), f"POST /AggregationJobs failed: {job_resp.status_code} {job_resp.text[:300]}"
        job_doc = job_resp.json()
        job_id = job_doc["id"]
        assert job_doc["sourceDatastream"] == {"id": src_id, "name": src_name}
        assert job_doc["targetDatastream"] == {"id": tgt_id, "name": tgt_name}

        use_local_psql = _is_local_compose_target(base_url)
        if use_local_psql:
            run_1 = _run_psql(f"CALL run_job({int(job_id)});")
            assert run_1.returncode == 0, run_1.stderr
            obs_1 = client.nav(
                f"Datastreams({format_id(tgt_id)})/Observations"
            )["value"]
            bucket_1 = [
                o for o in obs_1 if o.get("phenomenonTime") == bucket_time
            ]
            assert bucket_1 and bucket_1[0]["result"] == 30.0, obs_1

        src_renamed = f"{src_name}-renamed"
        tgt_renamed = f"{tgt_name}-renamed"
        assert (
            client.patch(
                src_url, json={"name": src_renamed}, headers=headers
            ).status_code
            == 200
        )
        assert (
            client.patch(
                tgt_url, json={"name": tgt_renamed}, headers=headers
            ).status_code
            == 200
        )

        after = client.nav(f"AggregationJobs({job_id})")
        assert after["sourceDatastream"] == {
            "id": src_id,
            "name": src_renamed,
        }
        assert after["targetDatastream"] == {
            "id": tgt_id,
            "name": tgt_renamed,
        }

        if use_local_psql:
            obs3_resp = client.post(
                "Observations",
                json={
                    "Datastream": {"@iot.id": src_id},
                    "phenomenonTime": t3,
                    "result": 5.0,
                },
                headers=headers,
            )
            assert obs3_resp.status_code == 201, obs3_resp.text[:300]
            run_2 = _run_psql(f"CALL run_job({int(job_id)});")
            assert run_2.returncode == 0, run_2.stderr
            obs_2 = client.nav(
                f"Datastreams({format_id(tgt_id)})/Observations"
            )["value"]
            bucket_2 = [
                o for o in obs_2 if o.get("phenomenonTime") == bucket_time
            ]
            assert bucket_2 and bucket_2[0]["result"] == 35.0, obs_2

        dup_resp = client.post(
            "AggregationJobs",
            json={
                "sourceDatastream": src_renamed,
                "targetDatastream": tgt_renamed,
                "bucketInterval": "1 hour",
                "aggregation": "SUM",
            },
        )
        assert dup_resp.status_code == 409, (
            f"Duplicate job after rename must return 409 Conflict; "
            f"got {dup_resp.status_code}: {dup_resp.text[:300]}"
        )

        patch_job = client.patch(
            f"AggregationJobs({job_id})", json={"enabled": True}
        )
        assert patch_job.status_code == 200, patch_job.text[:300]
        assert patch_job.json()["sourceDatastream"] == {
            "id": src_id,
            "name": src_renamed,
        }
        assert patch_job.json()["targetDatastream"] == {
            "id": tgt_id,
            "name": tgt_renamed,
        }
    finally:
        if job_id is not None and _is_local_compose_target(base_url):
            _run_psql(f"SELECT delete_job({int(job_id)});")
        for url in reversed(cleanup_urls):
            try:
                client.delete(url, headers=headers)
            except Exception:
                pass
