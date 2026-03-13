"""
Tests for helper functions in api/app/utils/utils.py

The function maps an observation result value to:
  - a result_type code (0–3)
  - an ordered list of column names
  - an ordered list of values to write into those columns

Result type codes:
    0 → numeric (int or float)
    1 → boolean
    2 → JSON / dict
    3 → string

Since the test directory is located outside the api package, the project root
and api directory are inserted into sys.path manually.

Usage:
    pytest test/test_utils_helper.py -v

Author: Vishmayraj
"""

import sys
import os
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
API_DIR = os.path.join(PROJECT_ROOT, 'api')

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, API_DIR)

from api.app.utils.utils import (
    safe_parse_datetime,
    get_result_type_and_column, 
    extract_iot_id)

class TestDatetimeParsing:
    def test_safe_parse_datetime_valid(self):
        assert safe_parse_datetime("2024-02-01T12:00Z") is not None

    def test_safe_parse_datetime_invalid(self):
        assert safe_parse_datetime("0000-00-00") is None
        assert safe_parse_datetime("N/A") is None

class TestIotIdExtraction:
    def test_extract_iot_id_valid(self):
        assert extract_iot_id({"@iot.id": 42}) == 42

    def test_extract_iot_id_invalid_structure(self):
        try:
            extract_iot_id("not a dict")
        except ValueError as e:
            assert "dict" in str(e)

    def test_extract_iot_id_missing_key(self):
        try:
            extract_iot_id({})
        except ValueError as e:
            assert "Missing '@iot.id'" in str(e)


# Helper Function
def assert_column_value(columns, values, column_name, expected_value):
    """Assert that a named column holds the expected value."""
    assert column_name in columns, (
        f"Expected column '{column_name}' to be present, got: {columns}"
    )
    idx = columns.index(column_name)
    assert values[idx] == expected_value, (
        f"Column '{column_name}': expected {expected_value!r}, got {values[idx]!r}"
    )

