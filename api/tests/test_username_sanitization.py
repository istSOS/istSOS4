# Copyright 2025 SUPSI
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

"""Tests for sanitize_username() -- the OIDC-only username normalizer.

Every case here is a real value actually observed live from a real
provider during this project's own manual testing, not a hypothetical:
Google's "Kinshuk S" (a bare display name, contains a space), ORCID's
"0009-0000-0287-1000" (the ORCID iD itself, all hyphens), and Microsoft's
email-as-username fallback. None of them pass validate_username()'s
^[a-zA-Z0-9_]{3,63}$ rule unmodified.

Pure function, no DB or mocking needed.
"""

import os
import sys
from pathlib import Path

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")

from app.utils.utils import sanitize_username, validate_username  # noqa: E402


def test_google_display_name_with_space():
    result = sanitize_username("Kinshuk S", "google-sub-1")
    assert result == "Kinshuk_S"
    assert validate_username(result)


def test_orcid_id_with_hyphens():
    result = sanitize_username("0009-0000-0287-1000", "orcid-sub-1")
    assert result == "0009_0000_0287_1000"
    assert validate_username(result)


def test_email_as_username():
    result = sanitize_username("lcb2024004@iiitl.ac.in", "ms-sub-1")
    assert result == "lcb2024004_iiitl_ac_in"
    assert validate_username(result)


def test_github_handle_with_hyphen():
    # GitHub's own username rules allow hyphens, which validate_username()
    # does not -- a real GitHub login could hit this even though GitHub
    # usernames look "clean" at a glance.
    result = sanitize_username("kinshuk-dev", "github-sub-1")
    assert result == "kinshuk_dev"
    assert validate_username(result)


def test_already_valid_username_is_unchanged():
    result = sanitize_username("already_valid_123", "sub-x")
    assert result == "already_valid_123"


def test_collapses_multiple_disallowed_characters_to_one_underscore():
    result = sanitize_username("a!!!b", "sub-x")
    assert result == "a_b"


def test_strips_leading_and_trailing_underscores():
    result = sanitize_username("-leading-and-trailing-", "sub-x")
    assert result == "leading_and_trailing"


def test_truncates_to_sixty_three_characters():
    result = sanitize_username("a" * 200, "sub-x")
    assert len(result) == 63
    assert validate_username(result)


def test_all_symbols_falls_back_to_hash_of_seed():
    # Nothing survives sanitization -- must not return a too-short or
    # empty string; validate_username() requires at least 3 characters.
    result = sanitize_username("!!!", "distinguishing-sub-id")
    assert validate_username(result)
    assert result.startswith("user_")


def test_empty_string_falls_back_to_hash_of_seed():
    result = sanitize_username("", "distinguishing-sub-id")
    assert validate_username(result)


def test_hash_fallback_is_deterministic_for_the_same_seed():
    # Same provider identity logging in twice (e.g. a retried callback)
    # must sanitize to the same fallback username both times.
    first = sanitize_username("", "same-sub-id")
    second = sanitize_username("", "same-sub-id")
    assert first == second


def test_hash_fallback_differs_for_different_seeds():
    # Two different pathological identities must not collide on the same
    # generated username.
    a = sanitize_username("!!!", "sub-a")
    b = sanitize_username("!!!", "sub-b")
    assert a != b


def test_unicode_only_display_name_falls_back_to_hash():
    # No ASCII alphanumerics survive at all -- must hit the hash fallback,
    # not return an empty or sub-3-character result.
    result = sanitize_username("北京", "sub-cjk-1")
    assert validate_username(result)
    assert result.startswith("user_")
