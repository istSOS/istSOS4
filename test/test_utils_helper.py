"""
Testing the freshly created helper functions.

Since the test directory is located in this way, I had to first find project root
where api directory was and then insert it to path manually.

Usage: Perform 'pytest -v' in the terminal

Author : Vishmayraj
"""

import sys
import os
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
API_DIR = os.path.join(PROJECT_ROOT, 'api')

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, API_DIR)

from api.app.utils.utils import safe_parse_datetime, extract_iot_id

def test_safe_parse_datetime_valid():
    assert safe_parse_datetime("2024-02-01T12:00Z") is not None

def test_safe_parse_datetime_invalid():
    assert safe_parse_datetime("0000-00-00") is None
    assert safe_parse_datetime("N/A") is None

def test_extract_iot_id_valid():
    assert extract_iot_id({"@iot.id": 42}) == 42

def test_extract_iot_id_invalid_structure():
    try:
        extract_iot_id("not a dict")
    except ValueError as e:
        assert "dict" in str(e)

def test_extract_iot_id_missing_key():
    try:
        extract_iot_id({})
    except ValueError as e:
        assert "Missing '@iot.id'" in str(e)