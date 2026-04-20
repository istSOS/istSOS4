"""
conftest.py — pytest configuration for the istSOS4 test suite.

Adds the project root and api/ to sys.path so all test files
can use clean absolute imports without managing paths themselves.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "api")))