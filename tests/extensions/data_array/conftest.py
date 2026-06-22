"""
conftest.py -- Data Array extension suite.

The Data Array extension is a FROST extension (NOT OGC 18-088). It runs under
NETWORK=0 and reads the SAME standard dataset the conformance suite uses, so we
re-export the standard `seed` fixture from tests/extensions/standard_seed.py.
(base_url / client / unique_name come from tests/extensions/conftest.py, which is
this tree's root conftest and puts tests/extensions/ on sys.path.)
"""
from standard_seed import seed  # noqa: F401  (re-exported as a pytest fixture)
