# Copyright 2026 SUPSI
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

"""Check real Compose interpolation without starting containers or reading .env."""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "dev_docker-compose.yml"
DEFAULTS = {"CENTER_LAT": 45.8693, "CENTER_LON": 8.9770, "SPREAD_DEG": 0.05}


class DummyDataComposeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docker = shutil.which("docker")
        if cls.docker is None:
            raise unittest.SkipTest("Docker Compose is required")
        result = subprocess.run(
            [cls.docker, "compose", "version"], capture_output=True, timeout=30
        )
        if result.returncode:
            raise unittest.SkipTest("Docker Compose is required")

    def resolve_geography(self, values):
        example = (ROOT / ".env.example").read_text(encoding="utf-8")
        settings = {}
        for line in example.splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                name, value = line.split("=", 1)
                settings[name] = value

        # Isolate the fixture from shell overrides and Compose environment files.
        environment = {
            name: value
            for name, value in os.environ.items()
            if name not in settings
            and name not in DEFAULTS
            and not name.startswith("COMPOSE_")
        }
        for name in DEFAULTS:
            settings.pop(name, None)
        settings.update(values)

        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "".join(f"{name}={value}\n" for name, value in settings.items()),
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    self.docker,
                    "compose",
                    "--project-directory",
                    str(ROOT),
                    "--env-file",
                    str(env_file),
                    "-f",
                    str(COMPOSE_FILE),
                    "config",
                    "--format",
                    "json",
                ],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        resolved = json.loads(result.stdout)["services"]["dummy_data"]["environment"]
        return {name: float(resolved[name]) for name in DEFAULTS}

    def test_missing_values_use_defaults(self):
        self.assertEqual(self.resolve_geography({}), DEFAULTS)

    def test_empty_values_use_defaults(self):
        self.assertEqual(
            self.resolve_geography({name: "" for name in DEFAULTS}), DEFAULTS
        )

    def test_custom_values_are_preserved(self):
        values = {
            "CENTER_LAT": "-33.8688",
            "CENTER_LON": "151.2093",
            "SPREAD_DEG": "0.2",
        }
        self.assertEqual(
            self.resolve_geography(values),
            {name: float(value) for name, value in values.items()},
        )

    def test_zero_values_are_preserved(self):
        self.assertEqual(
            self.resolve_geography({name: "0" for name in DEFAULTS}),
            {name: 0.0 for name in DEFAULTS},
        )

    def test_mixed_values_are_resolved_independently(self):
        self.assertEqual(
            self.resolve_geography({"CENTER_LON": "", "SPREAD_DEG": "0.2"}),
            {"CENTER_LAT": 45.8693, "CENTER_LON": 8.9770, "SPREAD_DEG": 0.2},
        )


if __name__ == "__main__":
    unittest.main()
