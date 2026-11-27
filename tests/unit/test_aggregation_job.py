# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from app.v1.endpoints.aggregation_job import (
    build_database_config,
    datastream_reference,
    serialize_job,
    set_job_enabled,
    validate_create_payload,
    validate_patch_payload,
)
from app.v1.endpoints.exceptions import BadRequest


class AggregationJobPayloadTest(unittest.TestCase):
    def test_minimal_payload_uses_job_defaults(self):
        payload = validate_create_payload(
            {
                "sourceDatastream": "raw-temperature",
                "targetDatastream": "temperature-10min",
            }
        )

        self.assertEqual("10 minutes", payload["bucketInterval"])
        self.assertEqual("10 minutes", payload["scheduleInterval"])
        self.assertEqual("Etc/GMT-1", payload["bucketTimezone"])
        self.assertEqual("Etc/GMT-1", payload["scheduleTimezone"])
        self.assertEqual("SUM", payload["aggregation"])
        self.assertTrue(payload["enabled"])

    def test_full_payload_builds_procedure_config(self):
        payload = validate_create_payload(
            {
                "sourceDatastream": "Praw_COL",
                "targetDatastream": "P_COL",
                "aggregation": "sum_null_as_one",
                "bucketInterval": "10 minutes",
                "bucketTimezone": "Europe/Zurich",
                "boundaryMode": "left_closed",
                "conversionFactor": 0.2,
                "bucketsToRecompute": 4,
                "resultQualityKey": "code",
                "resultQualityAllowed": [100, 200],
                "resultQualityMin": 100,
                "resultQualityMax": 255,
                "availabilityDatastreams": ["Ta_COL", "Ta_COL"],
                "availabilityMaxAge": "5 minutes",
                "emptyBucketPolicy": "zero_when_available",
                "emptyBucketResultQuality": 210,
                "systemTimeIncremental": True,
                "systemTimeOverlap": "1 minute",
                "featuresOfInterestId": 12,
                "resultType": 0,
                "scheduleInterval": "10 minutes",
                "scheduleDelay": "3 minutes",
                "fixedSchedule": True,
                "scheduleTimezone": "Europe/Zurich",
                "enabled": False,
            }
        )

        config = build_database_config(
            payload,
            {
                "Praw_COL": {"id": 1, "name": "Praw_COL"},
                "P_COL": {"id": 2, "name": "P_COL"},
                "Ta_COL": {"id": 3, "name": "Ta_COL"},
            },
        )

        self.assertEqual("SUM_NULL_AS_ONE", config["aggregation"])
        self.assertEqual(1, config["source_datastream_id"])
        self.assertEqual("Praw_COL", config["source_datastream_name"])
        self.assertEqual(2, config["target_datastream_id"])
        self.assertEqual("P_COL", config["target_datastream_name"])
        self.assertEqual([3], config["availability_datastream_ids"])
        self.assertEqual([100, 200], config["result_quality_allowed"])
        self.assertEqual("10 minutes", config["schedule_interval"])
        self.assertEqual("3 minutes", config["schedule_delay"])
        self.assertEqual(12, config["featuresofinterest_id"])

    def test_zero_when_available_requires_availability(self):
        with self.assertRaisesRegex(
            BadRequest, "requires availabilityDatastreams"
        ):
            validate_create_payload(
                {
                    "sourceDatastream": "raw",
                    "targetDatastream": "aggregate",
                    "emptyBucketPolicy": "zero_when_available",
                }
            )

    def test_quality_minimum_cannot_exceed_maximum(self):
        with self.assertRaisesRegex(BadRequest, "cannot be greater"):
            validate_create_payload(
                {
                    "sourceDatastream": "raw",
                    "targetDatastream": "aggregate",
                    "resultQualityMin": 10,
                    "resultQualityMax": 5,
                }
            )

    def test_unknown_create_property_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid keys"):
            validate_create_payload(
                {
                    "sourceDatastream": "raw",
                    "targetDatastream": "aggregate",
                    "procedure": "unsafe.procedure",
                }
            )

    def test_patch_accepts_only_a_boolean_enabled_value(self):
        self.assertEqual(
            {"enabled": False},
            validate_patch_payload({"enabled": False}),
        )

        with self.assertRaisesRegex(BadRequest, "must be a boolean"):
            validate_patch_payload({"enabled": 0})

    def test_datastream_reference_prefers_id_after_rename(self):
        config = {
            "source_datastream_id": 10,
            "source_datastream_name": "src",
            "target_datastream_id": 20,
            "target_datastream_name": "tgt",
        }
        by_id = {
            10: {"id": 10, "name": "src-renamed"},
            20: {"id": 20, "name": "tgt-renamed"},
            99: {"id": 99, "name": "src"},
        }
        by_name = {
            "src-renamed": by_id[10],
            "tgt-renamed": by_id[20],
            "src": by_id[99],
        }

        self.assertEqual(
            {"id": 10, "name": "src-renamed"},
            datastream_reference(config, "source", by_id, by_name),
        )
        self.assertEqual(
            {"id": 20, "name": "tgt-renamed"},
            datastream_reference(config, "target", by_id, by_name),
        )

    def test_datastream_reference_falls_back_to_name_for_legacy_jobs(self):
        config = {
            "source_datastream_name": "legacy-src",
            "target_datastream_name": "legacy-tgt",
        }
        by_name = {
            "legacy-src": {"id": 5, "name": "legacy-src"},
        }

        self.assertEqual(
            {"id": 5, "name": "legacy-src"},
            datastream_reference(config, "source", {}, by_name),
        )
        self.assertEqual(
            {"id": None, "name": "legacy-tgt"},
            datastream_reference(config, "target", {}, by_name),
        )

    def test_serialize_job_reflects_renamed_datastreams(self):
        row = {
            "job_id": 1000,
            "scheduled": True,
            "proc_schema": "sensorthings",
            "proc_name": "aggregate_datastream_1hour",
            "schedule_interval": "1 hour",
            "fixed_schedule": True,
            "initial_start": None,
            "next_start": None,
            "last_run_started_at": None,
            "last_successful_finish": None,
            "last_run_status": None,
            "job_status": None,
            "last_run_duration": None,
            "total_runs": 1,
            "total_successes": 1,
            "total_failures": 0,
        }
        config = {
            "source_datastream_id": 1,
            "source_datastream_name": "src",
            "target_datastream_id": 2,
            "target_datastream_name": "tgt",
        }
        by_id = {
            1: {"id": 1, "name": "src-renamed"},
            2: {"id": 2, "name": "tgt-renamed"},
        }
        serialized = serialize_job(row, config, by_id, {})
        self.assertEqual(
            {"id": 1, "name": "src-renamed"},
            serialized["sourceDatastream"],
        )
        self.assertEqual(
            {"id": 2, "name": "tgt-renamed"},
            serialized["targetDatastream"],
        )


class AggregationJobPatchSyncTest(unittest.IsolatedAsyncioTestCase):
    async def test_set_job_enabled_backfills_ids_and_refreshes_names(self):
        class FakeConn:
            def __init__(self):
                self.executed = []

            async def fetchrow(self, query, *args):
                if "timescaledb_information.jobs" in query:
                    return {
                        "config": (
                            '{"source_datastream_name": "src",'
                            ' "target_datastream_name": "tgt"}'
                        )
                    }
                if args == ("src",):
                    return {"id": 11, "name": "src"}
                if args == ("tgt",):
                    return {"id": 22, "name": "tgt"}
                return None

            async def execute(self, query, *args):
                self.executed.append((query, args))

        conn = FakeConn()
        await set_job_enabled(conn, 1000, False)
        self.assertEqual(1, len(conn.executed))
        query, args = conn.executed[0]
        self.assertIn("config => $3::text::jsonb", query)
        self.assertEqual(1000, args[0])
        self.assertFalse(args[1])
        self.assertIn('"source_datastream_id": 11', args[2])
        self.assertIn('"target_datastream_id": 22', args[2])


if __name__ == "__main__":
    unittest.main()
