"""
conftest.py -- pytest path bootstrap for the istSOS4 test suite.

Adds the project root and api/ to sys.path so every test file can use
clean absolute imports (e.g. `from app.sta2rest...`) without managing
paths themselves. Lives at the repo root so pytest picks it up
regardless of whether you run from root or from any subdirectory.
"""

import sys
import os

_ROOT = os.path.abspath(os.path.dirname(__file__))
_API = os.path.join(_ROOT, "api")

sys.path.insert(0, _ROOT)
sys.path.insert(0, _API)