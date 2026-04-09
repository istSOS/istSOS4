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
    extract_iot_id
)

# Helper functions
def assert_column_value(columns, values, column_name, expected_value):
    """Assert that a named column holds the expected value."""
    assert column_name in columns, (
        f"Expected column '{column_name}' to be present, got: {columns}"
    )
    idx = columns.index(column_name)
    assert values[idx] == expected_value, (
        f"Column '{column_name}': expected {expected_value!r}, got {values[idx]!r}"
    )


# safe_parse_datetime
class TestDatetimeParsing:
    """
    Tests for safe_parse_datetime(), which wraps dateutil.parser.parse
    and returns None instead of raising on invalid input.
    Covers valid ISO strings and inputs that should silently return None.
    """

    def test_safe_parse_datetime_valid(self):
        assert safe_parse_datetime("2024-02-01T12:00Z") is not None

    def test_safe_parse_datetime_invalid(self):
        assert safe_parse_datetime("0000-00-00") is None
        assert safe_parse_datetime("N/A") is None


# extract_iot_id
class TestIotIdExtraction:
    """
    Tests for extract_iot_id(), which pulls the '@iot.id' integer from
    an association dict. The function must raise ValueError for anything
    that is not a dict, missing the key, or has a non-integer value.
    """

    def test_extract_iot_id_valid(self):
        assert extract_iot_id({"@iot.id": 42}) == 42

    def test_extract_iot_id_invalid_structure(self):
        with pytest.raises(ValueError, match="dict"):
            extract_iot_id("not a dict")

    def test_extract_iot_id_missing_key(self):
        with pytest.raises(ValueError, match="Missing '@iot.id'"):
            extract_iot_id({})


# get_result_type_and_column 
class TestReturnStructure:
    """
    Verifies the shape of the return value regardless of input type.
    The function must always return a 3-tuple of (int, list, list)
    where values and columns have equal length and always contain
    exactly 4 entries, one per DB result column.
    """

    def test_returns_a_three_element_tuple(self):
        result = get_result_type_and_column("hello")
        assert isinstance(result, tuple) and len(result) == 3

    def test_first_element_is_an_integer(self):
        result_type, _, _ = get_result_type_and_column("hello")
        assert isinstance(result_type, int)

    def test_values_and_columns_have_equal_length(self):
        _, values, columns = get_result_type_and_column(42)
        assert len(values) == len(columns)

    def test_always_returns_exactly_four_columns(self):
        for sample in ["text", 1, 1.5, True, {"k": "v"}]:
            _, values, columns = get_result_type_and_column(sample)
            assert len(columns) == 4, f"Expected 4 columns for {sample!r}"
            assert len(values) == 4, f"Expected 4 values for {sample!r}"


class TestStringInput:
    """
    Verifies that a str value is classified as result_type = 3 and stored
    in resultString. All other columns must be None. No content-based
    inference should occur, a string that looks like a number or boolean
    must still be treated as a string.
    """

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
        """ "42" is a str. Therefore no content-based type inference should occur."""
        result_type, _, _ = get_result_type_and_column("42")
        assert result_type == 3

    def test_boolean_looking_string_stays_string(self):
        result_type, _, _ = get_result_type_and_column("true")
        assert result_type == 3

    def test_unicode_string(self):
        result_type, values, columns = get_result_type_and_column("温度")
        assert result_type == 3
        assert_column_value(columns, values, "resultString", "温度")


class TestDictInput:
    """
    Verifies that a dict value is classified as result_type = 2 and stored
    in resultJSON. All other columns must be None. The original dict
    reference must be preserved, no copying should occur since the
    DB layer expects to receive the exact object passed in.
    """

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

    def test_empty_dict(self):
        result_type, values, columns = get_result_type_and_column({})
        assert result_type == 2
        assert_column_value(columns, values, "resultJSON", {})

    def test_nested_dict(self):
        payload = {"sensor": {"id": 1, "value": 99.5}}
        result_type, values, columns = get_result_type_and_column(payload)
        assert result_type == 2
        assert_column_value(columns, values, "resultJSON", payload)

    def test_original_dict_reference_is_preserved(self):
        """No copy should be made, the DB layer receives the same object."""
        payload = {"k": "v"}
        _, values, columns = get_result_type_and_column(payload)
        idx = columns.index("resultJSON")
        assert values[idx] is payload


class TestBoolInput:
    """
    Verifies that a bool value is classified as result_type = 1 and stored
    in resultBoolean. The string form must be lower-case ('true'/'false')
    to match the SensorThings convention. Critically, since bool is a
    subclass of int in Python, this branch must be checked before the
    numeric branch to avoid misclassifying True/False as result_type = 0.
    """

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

    def test_result_string_is_lowercase_true(self):
        """SensorThings expects "true", not Python's "True"."""
        _, values, columns = get_result_type_and_column(True)
        assert_column_value(columns, values, "resultString", "true")

    def test_result_string_is_lowercase_false(self):
        _, values, columns = get_result_type_and_column(False)
        assert_column_value(columns, values, "resultString", "false")

    def test_other_columns_are_none(self):
        _, values, columns = get_result_type_and_column(True)
        assert_column_value(columns, values, "resultNumber", None)
        assert_column_value(columns, values, "resultJSON", None)


class TestIntInput:
    """
    Verifies that an int value is classified as result_type = 0 and stored
    in resultNumber. resultString receives the str() form for text-based
    queries. Covers positive, negative, and zero, including the falsy
    zero trap where a truthiness check would incorrectly skip the branch.
    """

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

    def test_zero_is_numeric_not_falsy(self):
        """0 is falsy in Python, ensure isinstance is used, not truthiness."""
        result_type, values, columns = get_result_type_and_column(0)
        assert result_type == 0
        assert_column_value(columns, values, "resultNumber", 0)


class TestFloatInput:
    """
    Verifies that a float value is classified as result_type = 0 alongside
    int, and stored in resultNumber. resultString receives str(value),
    which preserves the decimal point (str(1.0) -> "1.0"). Covers
    positive, negative, and zero floats.
    """

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


class TestInvalidInput:
    """
    Verifies that any unsupported type raises Exception with the message
    'Cannot cast result to a valid type'. Covers None, list, tuple, set,
    bytes, and a custom object, basically anything the function has no branch for
    must fail loudly rather than silently returning wrong data.
    """

    def test_none_raises(self):
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column(None)

    def test_list_raises(self):
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column([1, 2, 3])

    def test_tuple_raises(self):
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column((1, 2))

    def test_set_raises(self):
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column({1, 2, 3})

    def test_bytes_raises(self):
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column(b"bytes")

    def test_custom_object_raises(self):
        class Foo:
            """Dummy class representing an unsupported result type."""
            pass
        with pytest.raises(Exception, match="Cannot cast result to a valid type"):
            get_result_type_and_column(Foo())


class TestColumnOrdering:
    """
    Verifies that the first column in the returned list is always the
    active (non-None) one. Callers zip values and columns together and
    rely on this ordering contract, if it breaks, the wrong column gets
    written to in the DB.
    """

    def test_first_column_is_result_string_for_string(self):
        _, _, columns = get_result_type_and_column("hello")
        assert columns[0] == "resultString"

    def test_first_column_is_result_json_for_dict(self):
        _, _, columns = get_result_type_and_column({"k": "v"})
        assert columns[0] == "resultJSON"

    def test_first_column_is_result_boolean_for_bool(self):
        _, _, columns = get_result_type_and_column(True)
        assert columns[0] == "resultBoolean"

    def test_first_column_is_result_number_for_int(self):
        _, _, columns = get_result_type_and_column(42)
        assert columns[0] == "resultNumber"

    def test_first_column_is_result_number_for_float(self):
        _, _, columns = get_result_type_and_column(3.14)
        assert columns[0] == "resultNumber"

    def test_first_value_is_never_none(self):
        """values[0] must always be the actual payload, never None."""
        cases = ["hello", {"k": "v"}, True, 42, 3.14]
        for val in cases:
            _, values, _ = get_result_type_and_column(val)
            assert values[0] is not None, (
                f"values[0] was None for input {val!r}"
            )
