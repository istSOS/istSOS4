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

"""Tests for the external-identity-provider integration: app.oidc_providers
and app.v1.endpoints.create.oidc_login.

Two layers are covered, and they prove different things -- kept explicit so
a passing suite here is never mistaken for more than it actually shows:

1. Claim normalization (TestNormalizeClaims) -- for all five providers
   (Google, Microsoft, ORCID, edu-ID, GitHub), against fixture payloads
   shaped like each provider's real documented claims. This is genuinely
   unit-level and needs no network access.

2. Full login -> session -> callback -> provisioning flow
   (TestOidcLoginRoute / TestOidcCallbackRoute) -- exercised through
   FastAPI's TestClient against the real app, with Authlib's
   `authorize_access_token` and `get` methods replaced by mocks (so no
   real network call to Google/GitHub/etc. happens) and the database layer
   mocked the same way test_oidc_username_collision.py already does.

What this suite does NOT prove: that a real browser can complete a real
consent screen against Google/Microsoft/GitHub/ORCID/edu-ID and land back
here with a genuine authorization code. That requires real registered app
credentials and a real user clicking "allow" -- there is no way to
exercise that from an automated test in this environment. The redirect
half (login -> real provider) was separately verified live against
Google's and GitHub's actual endpoints (see the session transcript, not a
pytest run) -- this file does not repeat that.
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="function")

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("AUTHORIZATION", "1")

from app.oidc_providers import normalize_claims  # noqa: E402

# ---------------------------------------------------------------------------
# Layer 1: claim normalization, one fixture per provider's real claim shape.
# ---------------------------------------------------------------------------


class _FakeGithubClient:
    """Stands in for the Authlib client's `.get()` -- GitHub has no
    id_token, so normalize_claims makes two REST calls through this.
    Also usable as a full route-level fake client (authorize_redirect /
    authorize_access_token), since the GitHub route tests need the same
    object to play both roles."""

    def __init__(self, profile: dict, emails: list[dict] | None = None):
        self._profile = profile
        self._emails = emails or []
        self.authorize_redirect = AsyncMock(
            side_effect=_fake_authorize_redirect
        )
        self.authorize_access_token = AsyncMock(
            return_value={"access_token": "gho_faketoken"}
        )

    async def get(self, path: str, token=None):
        resp = MagicMock()
        if path == "user":
            resp.json.return_value = self._profile
        elif path == "user/emails":
            resp.json.return_value = self._emails
        return resp


async def _fake_authorize_redirect(request, redirect_uri):
    from starlette.responses import RedirectResponse

    return RedirectResponse("https://example-idp.test/authorize?fake=1")


def _make_fake_oidc_client(token_result: dict):
    """A MagicMock standing in for an Authlib OIDC client, with both
    methods the routes actually call made properly awaitable -- a bare
    MagicMock's attributes aren't coroutines, so `await client.x()` raises
    TypeError unless each one is explicitly an AsyncMock."""
    client = MagicMock()
    client.authorize_redirect = AsyncMock(side_effect=_fake_authorize_redirect)
    client.authorize_access_token = AsyncMock(return_value=token_result)
    return client


async def test_normalize_claims_google():
    # Real Google id_token claims: sub, email, name, email_verified.
    token = {
        "userinfo": {
            "sub": "108234982374982374",
            "email": "jdoe@gmail.com",
            "email_verified": True,
            "name": "Jane Doe",
        }
    }
    result = await normalize_claims("google", token, client=None)
    assert result == {
        # no preferred_username on Google -> falls to email (ranked above
        # name: a stable, meaningful identifier vs. a raw display string)
        "username": "jdoe@gmail.com",
        "email": "jdoe@gmail.com",
        "auth_provider": "google",
        "external_sub_id": "108234982374982374",
    }


async def test_normalize_claims_microsoft():
    # Microsoft/Entra ID commonly returns preferred_username (often the
    # UPN/email) alongside name.
    token = {
        "userinfo": {
            "sub": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "email": "jdoe@outlook.com",
            "name": "Jane Doe",
            "preferred_username": "jdoe@outlook.com",
        }
    }
    result = await normalize_claims("microsoft", token, client=None)
    assert result == {
        "username": "jdoe@outlook.com",  # preferred_username wins
        "email": "jdoe@outlook.com",
        "auth_provider": "microsoft",
        "external_sub_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    }


async def test_normalize_claims_orcid_no_email_no_preferred_username():
    # ORCID's sub IS the ORCID iD itself; no preferred_username, and email
    # is frequently absent/private -- this is the fallback chain's real
    # reason for existing.
    token = {
        "userinfo": {
            "sub": "0000-0001-2345-6789",
            "name": "Jane Doe",
        }
    }
    result = await normalize_claims("orcid", token, client=None)
    assert result == {
        "username": "Jane Doe",
        "email": None,
        "auth_provider": "orcid",
        "external_sub_id": "0000-0001-2345-6789",
    }


async def test_normalize_claims_orcid_falls_back_to_sub_with_no_name_either():
    # Worst case: only sub is guaranteed by the OIDC spec. Must not KeyError.
    token = {"userinfo": {"sub": "0000-0002-9999-1111"}}
    result = await normalize_claims("orcid", token, client=None)
    assert result["username"] == "0000-0002-9999-1111"
    assert result["external_sub_id"] == "0000-0002-9999-1111"


async def test_normalize_claims_eduid():
    token = {
        "userinfo": {
            "sub": "eduid-sub-abc123",
            "email": "jane.doe@supsi.ch",
            "name": "Jane Doe",
        }
    }
    result = await normalize_claims("eduid", token, client=None)
    assert result == {
        # no preferred_username -> falls to email, ranked above name
        "username": "jane.doe@supsi.ch",
        "email": "jane.doe@supsi.ch",
        "auth_provider": "eduid",
        "external_sub_id": "eduid-sub-abc123",
    }


async def test_normalize_claims_github_public_email():
    client = _FakeGithubClient(
        profile={"id": 987654, "login": "jdoe-gh", "email": "jdoe@example.com"}
    )
    result = await normalize_claims("github", token={}, client=client)
    assert result == {
        "username": "jdoe-gh",
        "email": "jdoe@example.com",
        "auth_provider": "github",
        "external_sub_id": "987654",
    }


async def test_normalize_claims_github_private_email_falls_back_to_emails_endpoint():
    # GitHub omits `email` from /user entirely when the profile email is
    # private -- this is the exact case the /user/emails fallback exists for.
    client = _FakeGithubClient(
        profile={"id": 987654, "login": "jdoe-gh", "email": None},
        emails=[
            {"email": "secondary@example.com", "primary": False, "verified": True},
            {"email": "primary@example.com", "primary": True, "verified": True},
        ],
    )
    result = await normalize_claims("github", token={}, client=client)
    assert result["email"] == "primary@example.com"
    assert result["username"] == "jdoe-gh"
    assert result["external_sub_id"] == "987654"


async def test_normalize_claims_github_no_verified_primary_email_at_all():
    # No primary email in the list -> None, not a crash.
    client = _FakeGithubClient(
        profile={"id": 1, "login": "ghost", "email": None},
        emails=[{"email": "x@example.com", "primary": False, "verified": True}],
    )
    result = await normalize_claims("github", token={}, client=client)
    assert result["email"] is None


# ---------------------------------------------------------------------------
# Layer 2: the actual routes, via TestClient, with Authlib's client and the
# DB layer both mocked. Registers one fake OIDC-shaped provider and one
# fake GitHub-shaped provider so both normalize_claims branches get
# exercised through the real route code, not just called directly.
# ---------------------------------------------------------------------------


def _fake_pool_for_provisioning(fetchrow_result=None, insert_result=None):
    """Mimics test_oidc_username_collision.py's mock pool, extended to
    also back get_user_by_provider_sub's SELECT.

    Both get_user_by_provider_sub's lookup and create_pending_oidc_user's
    INSERT run fetchrow on the same connection, in that order -- when a
    test needs to exercise both (an unknown identity that then gets
    provisioned), insert_result supplies the second call's return value
    via side_effect; a bare fetchrow_result alone covers tests that only
    ever reach the lookup.
    """

    @asynccontextmanager
    async def fake_transaction():
        yield None

    mock_conn = AsyncMock()
    mock_conn.transaction = MagicMock(side_effect=fake_transaction)
    mock_conn.execute = AsyncMock(return_value=None)
    if insert_result is not None:
        mock_conn.fetchrow = AsyncMock(
            side_effect=[fetchrow_result, insert_result]
        )
    else:
        mock_conn.fetchrow = AsyncMock(return_value=fetchrow_result)

    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    mock_pool = MagicMock()
    mock_pool.acquire = mock_acquire
    return mock_pool


@pytest.fixture
def app_client():
    """A TestClient against the real app, with two fake providers injected
    into the live provider registry for the duration of the test."""
    from app.main import app
    import app.oidc_providers as providers_module
    import app.v1.endpoints.create.oidc_login as route_module
    from authlib.integrations.starlette_client import OAuth

    test_oauth = OAuth()
    test_oauth.register(
        name="testoidc",
        client_id="fake-id",
        client_secret="fake-secret",
        authorize_url="https://example-idp.test/authorize",
        access_token_url="https://example-idp.test/token",
        client_kwargs={"scope": "openid email profile"},
    )
    test_oauth.register(
        name="github",
        client_id="fake-gh-id",
        client_secret="fake-gh-secret",
        authorize_url="https://github.com/login/oauth/authorize",
        access_token_url="https://github.com/login/oauth/access_token",
        api_base_url="https://api.github.com/",
        client_kwargs={"scope": "read:user user:email"},
    )

    with patch.object(route_module, "oauth", test_oauth), patch.object(
        route_module, "ENABLED_PROVIDERS", ["testoidc", "github"]
    ), patch.object(
        providers_module, "ENABLED_PROVIDERS", ["testoidc", "github"]
    ):
        from fastapi.testclient import TestClient

        yield TestClient(app)


def test_login_requires_dataset_id_and_policy_id(app_client):
    r = app_client.get(
        "/istsos4/v1.1/auth/testoidc/login", follow_redirects=False
    )
    assert r.status_code == 422  # missing required query params


def test_login_rejects_unconfigured_provider(app_client):
    r = app_client.get(
        "/istsos4/v1.1/auth/not_a_real_provider/login"
        "?dataset_id=ds://x&odrl_policy_id=odrl://y&requested_role=viewer",
        follow_redirects=False,
    )
    assert r.status_code == 404
    assert "not_a_real_provider" in r.json()["detail"]


def test_login_redirects_and_stores_selection_in_session(app_client):
    r = app_client.get(
        "/istsos4/v1.1/auth/testoidc/login"
        "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
        follow_redirects=False,
    )
    assert r.status_code == 302
    assert "example-idp.test/authorize" in r.headers["location"]
    # Session cookie must be set -- this is what carries the selection
    # through to the callback.
    assert "session" in r.cookies


def test_callback_without_prior_login_returns_400(app_client):
    # Hitting /callback directly, with no session state from /login first.
    import app.v1.endpoints.create.oidc_login as route_module

    fake_client = _make_fake_oidc_client(
        {"userinfo": {"sub": "x", "name": "x"}}
    )

    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        r = app_client.get(
            "/istsos4/v1.1/auth/testoidc/callback?code=abc&state=xyz",
            follow_redirects=False,
        )
    assert r.status_code == 400
    assert "dataset" in r.json()["detail"].lower()


def test_callback_new_identity_creates_pending_user_and_returns_202(app_client):
    import app.db.oidc_user_crud as crud_module
    import app.v1.endpoints.create.oidc_login as route_module

    fake_client = _make_fake_oidc_client(
        {
            "userinfo": {
                "sub": "new-sub-1",
                "email": "new@example.com",
                "name": "New User",
            }
        }
    )

    # Session round trip first (real, not mocked) via /login.
    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        app_client.get(
            "/istsos4/v1.1/auth/testoidc/login"
            "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
            follow_redirects=False,
        )

        # get_user_by_provider_sub -> None (unknown identity), then the
        # INSERT inside create_pending_oidc_user returns the new row.
        no_match_pool = _fake_pool_for_provisioning(
            fetchrow_result=None,
            insert_result={
                "id": 42,
                "username": "New User",
                "role": "pending",
                "uri": "/Users(42)",
                "auth_provider": "testoidc",
                "external_sub_id": "new-sub-1",
                "dataset_id": "ds://climate",
                "odrl_policy_id": "odrl://supsi",
            },
        )
        with patch.object(
            crud_module, "get_pool", AsyncMock(return_value=no_match_pool)
        ):
            r = app_client.get(
                "/istsos4/v1.1/auth/testoidc/callback?code=abc&state=xyz",
                follow_redirects=False,
            )

    assert r.status_code == 202
    assert "administrator must approve" in r.json()["message"].lower()


def test_callback_existing_pending_identity_returns_202(app_client):
    import app.db.oidc_user_crud as crud_module
    import app.v1.endpoints.create.oidc_login as route_module

    fake_client = _make_fake_oidc_client(
        {
            "userinfo": {
                "sub": "existing-sub-1",
                "email": "existing@example.com",
                "name": "Existing User",
            }
        }
    )

    existing_row = {
        "id": 7,
        "username": "existing_user",
        "role": "pending",
        "uri": "/Users(7)",
        "auth_provider": "testoidc",
        "external_sub_id": "existing-sub-1",
        "dataset_id": "ds://climate",
        "odrl_policy_id": "odrl://supsi",
    }

    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        app_client.get(
            "/istsos4/v1.1/auth/testoidc/login"
            "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
            follow_redirects=False,
        )

        pool = _fake_pool_for_provisioning(fetchrow_result=existing_row)
        with patch.object(crud_module, "get_pool", AsyncMock(return_value=pool)):
            r = app_client.get(
                "/istsos4/v1.1/auth/testoidc/callback?code=abc&state=xyz",
                follow_redirects=False,
            )

    assert r.status_code == 202
    assert "pending administrator" in r.json()["message"].lower()


def test_callback_existing_approved_identity_issues_access_token(app_client):
    import app.db.oidc_user_crud as crud_module
    import app.v1.endpoints.create.oidc_login as route_module

    fake_client = _make_fake_oidc_client(
        {
            "userinfo": {
                "sub": "approved-sub-1",
                "email": "approved@example.com",
                "name": "Approved User",
            }
        }
    )

    existing_row = {
        "id": 9,
        "username": "approved_user",
        "role": "viewer",
        "uri": "/Users(9)",
        "auth_provider": "testoidc",
        "external_sub_id": "approved-sub-1",
        "dataset_id": "ds://climate",
        "odrl_policy_id": "odrl://supsi",
    }

    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        app_client.get(
            "/istsos4/v1.1/auth/testoidc/login"
            "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
            follow_redirects=False,
        )

        pool = _fake_pool_for_provisioning(fetchrow_result=existing_row)
        with patch.object(crud_module, "get_pool", AsyncMock(return_value=pool)):
            r = app_client.get(
                "/istsos4/v1.1/auth/testoidc/callback?code=abc&state=xyz",
                follow_redirects=False,
            )

    assert r.status_code == 200
    body = r.json()
    assert body["token_type"] == "bearer"
    assert isinstance(body["access_token"], str) and body["access_token"]


def test_callback_username_collision_returns_409(app_client):
    import app.db.oidc_user_crud as crud_module
    import app.v1.endpoints.create.oidc_login as route_module
    from asyncpg.exceptions import UniqueViolationError

    fake_client = _make_fake_oidc_client(
        {
            "userinfo": {
                "sub": "collide-sub-1",
                "email": "collide@example.com",
                "name": "existing_local_user",
            }
        }
    )

    exc = UniqueViolationError(
        "duplicate key value violates unique constraint 'User_username_key'"
    )
    exc.constraint_name = "User_username_key"

    @asynccontextmanager
    async def fake_transaction():
        yield None

    mock_conn = AsyncMock()
    mock_conn.transaction = MagicMock(side_effect=fake_transaction)
    mock_conn.execute = AsyncMock(return_value=None)
    # First fetchrow: get_user_by_provider_sub -> no match. Second
    # fetchrow: the INSERT itself -> raises the collision.
    mock_conn.fetchrow = AsyncMock(side_effect=[None, exc])

    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    mock_pool = MagicMock()
    mock_pool.acquire = mock_acquire

    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        app_client.get(
            "/istsos4/v1.1/auth/testoidc/login"
            "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
            follow_redirects=False,
        )

        with patch.object(
            crud_module, "get_pool", AsyncMock(return_value=mock_pool)
        ):
            r = app_client.get(
                "/istsos4/v1.1/auth/testoidc/callback?code=abc&state=xyz",
                follow_redirects=False,
            )

    assert r.status_code == 409
    assert "already taken" in r.json()["detail"].lower()


def test_callback_github_uses_rest_api_not_id_token(app_client):
    """GitHub's fake client has no userinfo in the token -- if the route
    tried to read token['userinfo'] for GitHub it would KeyError/produce a
    wrong username. This proves the REST-call branch is what actually runs."""
    import app.db.oidc_user_crud as crud_module
    import app.v1.endpoints.create.oidc_login as route_module

    fake_client = _FakeGithubClient(
        profile={"id": 555, "login": "gh_user", "email": "gh@example.com"}
    )
    fake_client.authorize_access_token = AsyncMock(
        return_value={"access_token": "gho_faketoken"}  # no userinfo/id_token
    )

    with patch.object(
        route_module.oauth, "create_client", return_value=fake_client
    ):
        app_client.get(
            "/istsos4/v1.1/auth/github/login"
            "?dataset_id=ds://climate&odrl_policy_id=odrl://supsi&requested_role=viewer",
            follow_redirects=False,
        )

        no_match_pool = _fake_pool_for_provisioning(
            fetchrow_result=None,
            insert_result={
                "id": 55,
                "username": "gh_user",
                "role": "pending",
                "uri": "/Users(55)",
                "auth_provider": "github",
                "external_sub_id": "555",
                "dataset_id": "ds://climate",
                "odrl_policy_id": "odrl://supsi",
            },
        )
        with patch.object(
            crud_module, "get_pool", AsyncMock(return_value=no_match_pool)
        ):
            r = app_client.get(
                "/istsos4/v1.1/auth/github/callback?code=abc&state=xyz",
                follow_redirects=False,
            )

    assert r.status_code == 202
