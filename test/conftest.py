"""
test/conftest.py -- path bootstrap for running pytest from inside test/.

When pytest is invoked from the test/ directory directly its rootdir
becomes test/ and the root conftest.py may not be loaded. This file
re-applies the same sys.path logic so imports resolve identically
from both invocation points.
"""

import os
import sys

_TEST_DIR = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_TEST_DIR, ".."))
_API = os.path.join(_ROOT, "api")

for _path in (_ROOT, _API):
    if _path not in sys.path:
        sys.path.insert(0, _path)
