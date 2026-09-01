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


if __name__ == "__main__":
    unittest.main()
