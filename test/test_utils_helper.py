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


class TestStringInput:

    def test_result_type_is_3(self):
        result_type, _, _ = get_result_type_and_column("hello")
        assert result_type == 3

    def test_result_string_holds_the_value(self):
        _, values, columns = get_result_type_and_column("hello")
        assert_column_value(columns, values, "resultString", "hello")

    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column("hello")
        assert_column_value(columns, values, "resultBoolean", None)
        assert_column_value(columns, values, "resultNumber", None)
        assert_column_value(columns, values, "resultJSON", None)

    def test_empty_string(self):
        result_type, values, columns = get_result_type_and_column("")
        assert result_type == 3
        assert_column_value(columns, values, "resultString", "")

    def test_numeric_looking_string_stays_string(self):
        """ "42" is a str, therefore no content based type inference should occur."""
        result_type, _, _ = get_result_type_and_column("42")
        assert result_type == 3


class TestDictInput:

    def test_result_type_is_2(self):
        result_type, _, _ = get_result_type_and_column({"temp": 22})
        assert result_type == 2

    def test_result_json_holds_the_dict(self):
        payload = {"temp": 22, "unit": "C"}
        _, values, columns = get_result_type_and_column(payload)
        assert_column_value(columns, values, "resultJSON", payload)

    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column({"a": 1})
        assert_column_value(columns, values, "resultBoolean", None)
        assert_column_value(columns, values, "resultNumber", None)
        assert_column_value(columns, values, "resultString", None)


    def test_original_dict_reference_is_preserved(self):
        """No copy should be made, the DB layer receives the same object."""
        payload = {"k": "v"}
        _, values, columns = get_result_type_and_column(payload)
        idx = columns.index("resultJSON")
        assert values[idx] is payload


class TestBoolInput:

    def test_result_type_is_1_for_true(self):
        result_type, _, _ = get_result_type_and_column(True)
        assert result_type == 1

    def test_result_type_is_1_for_false(self):
        result_type, _, _ = get_result_type_and_column(False)
        assert result_type == 1

    def test_true_is_not_classified_as_numeric(self):
        """bool is a subclass of int. Thus, True must not become result_type=0."""
        result_type, _, _ = get_result_type_and_column(True)
        assert result_type != 0, (
            "True was classified as numeric. "
            "Check that the bool branch appears before int/float."
        )

    def test_false_is_not_classified_as_numeric(self):
        result_type, _, _ = get_result_type_and_column(False)
        assert result_type != 0

    def test_result_boolean_holds_true(self):
        _, values, columns = get_result_type_and_column(True)
        assert_column_value(columns, values, "resultBoolean", True)

    def test_result_boolean_holds_false(self):
        _, values, columns = get_result_type_and_column(False)
        assert_column_value(columns, values, "resultBoolean", False)


    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column(True)
        assert_column_value(columns, values, "resultNumber", None)
        assert_column_value(columns, values, "resultJSON", None)


class TestIntInput:

    def test_result_type_is_0_for_positive_int(self):
        result_type, _, _ = get_result_type_and_column(42)
        assert result_type == 0

    def test_result_type_is_0_for_negative_int(self):
        result_type, _, _ = get_result_type_and_column(-7)
        assert result_type == 0

    def test_result_number_holds_the_value(self):
        _, values, columns = get_result_type_and_column(42)
        assert_column_value(columns, values, "resultNumber", 42)

    def test_result_string_holds_string_form(self):
        _, values, columns = get_result_type_and_column(42)
        assert_column_value(columns, values, "resultString", "42")

    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column(42)
        assert_column_value(columns, values, "resultBoolean", None)
        assert_column_value(columns, values, "resultJSON", None)


class TestFloatInput:

    def test_result_type_is_0_for_float(self):
        result_type, _, _ = get_result_type_and_column(3.14)
        assert result_type == 0

    def test_result_number_holds_the_float(self):
        _, values, columns = get_result_type_and_column(3.14)
        assert_column_value(columns, values, "resultNumber", 3.14)

    def test_result_string_holds_string_form_of_float(self):
        """str(1.0) -> "1.0", preserving the decimal point."""
        _, values, columns = get_result_type_and_column(1.0)
        assert_column_value(columns, values, "resultString", "1.0")

    def test_negative_float(self):
        result_type, values, columns = get_result_type_and_column(-0.5)
        assert result_type == 0
        assert_column_value(columns, values, "resultNumber", -0.5)

    def test_zero_float(self):
        result_type, values, columns = get_result_type_and_column(0.0)
        assert result_type == 0
        assert_column_value(columns, values, "resultNumber", 0.0)

    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column(3.14)
        assert_column_value(columns, values, "resultBoolean", None)
        assert_column_value(columns, values, "resultJSON", None)
