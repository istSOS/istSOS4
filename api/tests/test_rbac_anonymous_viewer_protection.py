"""Regression tests for RBAC protections in anonymous viewer mode.

These tests verify that sensitive authorization metadata endpoints keep
requiring authentication even when ANONYMOUS_VIEWER is enabled.
"""

import importlib
import os
import sys
from pathlib import Path

from fastapi.params import Depends


# Ensure api/ is on sys.path so imports like `app.*` resolve.
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)


def _reload_read_module(module_name: str):
    """Reload a read endpoint module after changing auth-related env vars."""
    for name in [module_name, "app"]:
        if name in sys.modules:
            del sys.modules[name]
    return importlib.import_module(module_name)


def test_users_read_requires_auth_even_with_anonymous_viewer_enabled():
    os.environ["AUTHORIZATION"] = "1"
    os.environ["ANONYMOUS_VIEWER"] = "1"

    user_module = _reload_read_module("app.v1.endpoints.read.user")
    oauth_module = importlib.import_module("app.oauth")

    assert isinstance(user_module.user, Depends)
    assert user_module.user.dependency is oauth_module.get_current_user


def test_policies_read_requires_auth_even_with_anonymous_viewer_enabled():
    os.environ["AUTHORIZATION"] = "1"
    os.environ["ANONYMOUS_VIEWER"] = "1"

    policy_module = _reload_read_module("app.v1.endpoints.read.policy")
    oauth_module = importlib.import_module("app.oauth")

    assert isinstance(policy_module.user, Depends)
    assert policy_module.user.dependency is oauth_module.get_current_user
