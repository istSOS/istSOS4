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

"""External identity provider registry: Google, Microsoft, GitHub, ORCID,
SWITCH edu-ID.

This is the piece oidc_user_crud.py's own docstring flags as missing: the
actual handshake with a real external identity provider. That module only
ever received hand-built claims from a unit test; the routes in
app.v1.endpoints.create.oidc_login are the first real caller.

Provider shapes are not uniform, and that split drives the design here:

* Google, Microsoft, ORCID, and edu-ID are true OpenID Connect providers --
  each publishes a discovery document, Authlib fetches it via
  ``server_metadata_url``, and the identity claims (``sub``, ``email``,
  ``name``) arrive in a signed ``id_token`` that Authlib validates against
  the provider's live JWKS for us. One code path handles all four.

* GitHub is OAuth2 only -- it never issues an id_token or standard claims.
  Its identity has to be fetched with a follow-up REST call to
  ``GET /user`` (and ``/user/emails`` if the profile email is private), so
  it's registered with explicit endpoint URLs instead of a discovery URL
  and normalized separately in ``normalize_claims``.

A provider is only registered if both its CLIENT_ID and CLIENT_SECRET env
vars are set, so a deployment can enable any subset of the five without
code changes -- unset ones are silently skipped, not errors.
"""

import os

from authlib.integrations.starlette_client import OAuth

oauth = OAuth()

# env var prefix -> provider name used everywhere else (URLs, DB rows, etc.)
_PROVIDER_ENV_PREFIX = {
    "google": "GOOGLE",
    "microsoft": "MICROSOFT",
    "github": "GITHUB",
    "orcid": "ORCID",
    "eduid": "EDUID",
}

# OIDC discovery documents for the four real OIDC providers. Microsoft and
# ORCID are overridable via env var because real deployments often need a
# specific tenant (Microsoft: "organizations" vs "common") or the sandbox
# environment (ORCID) instead of the production default.
#
# `os.getenv(key, default)` only falls back to `default` when the key is
# completely absent -- but Docker Compose substitutes an unset `${VAR}` as
# an empty string, not an absent key, so the container's actual
# environment has e.g. MICROSOFT_DISCOVERY_URL="" (present, empty), not
# unset. `os.getenv(key, default)` sees that as "set" and returns "",
# silently registering the client with no discovery URL at all -- Authlib
# then fails at request time with "Missing authorize_url value" rather
# than at import time, which is what made this take a live test to catch.
# `os.getenv(key) or default` treats None and "" the same way.
_OIDC_DISCOVERY_URLS = {
    "google": "https://accounts.google.com/.well-known/openid-configuration",
    "microsoft": os.getenv("MICROSOFT_DISCOVERY_URL")
    or "https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration",
    "orcid": os.getenv("ORCID_DISCOVERY_URL")
    or "https://orcid.org/.well-known/openid-configuration",
    "eduid": "https://login.eduid.ch/.well-known/openid-configuration",
}

# Populated below, at import time, with whichever providers have both env
# vars set. app.v1.endpoints.create.oidc_login imports this to validate
# {provider} path params and to report what's actually enabled.
ENABLED_PROVIDERS: list[str] = []

for _name, _env_prefix in _PROVIDER_ENV_PREFIX.items():
    _client_id = os.getenv(f"{_env_prefix}_CLIENT_ID")
    _client_secret = os.getenv(f"{_env_prefix}_CLIENT_SECRET")
    if not _client_id or not _client_secret:
        continue  # provider not configured for this deployment

    if _name == "github":
        oauth.register(
            name="github",
            client_id=_client_id,
            client_secret=_client_secret,
            authorize_url="https://github.com/login/oauth/authorize",
            access_token_url="https://github.com/login/oauth/access_token",
            api_base_url="https://api.github.com/",
            client_kwargs={"scope": "read:user user:email"},
        )
    else:
        oauth.register(
            name=_name,
            client_id=_client_id,
            client_secret=_client_secret,
            server_metadata_url=_OIDC_DISCOVERY_URLS[_name],
            client_kwargs={"scope": "openid email profile"},
        )

    ENABLED_PROVIDERS.append(_name)


async def normalize_claims(provider: str, token: dict, client) -> dict:
    """Map a provider's raw token/claims into the shape
    ``create_pending_oidc_user`` expects: ``username``, ``email``,
    ``auth_provider``, ``external_sub_id``.

    GitHub has no id_token, so its identity comes from a REST call instead
    of ``token["userinfo"]``; every other registered provider is real OIDC,
    so Authlib has already validated and attached ``userinfo`` to ``token``
    during ``authorize_access_token``.
    """
    if provider == "github":
        resp = await client.get("user", token=token)
        profile = resp.json()

        email = profile.get("email")
        if not email:
            # Private-email GitHub accounts omit it from /user; the
            # verified primary address lives in /user/emails instead.
            emails_resp = await client.get("user/emails", token=token)
            primary = next(
                (e for e in emails_resp.json() if e.get("primary")), None
            )
            email = primary["email"] if primary else None

        return {
            "username": profile["login"],
            "email": email,
            "auth_provider": "github",
            "external_sub_id": str(profile["id"]),
        }

    userinfo = token.get("userinfo") or {}
    # preferred_username/email/name aren't guaranteed present on every
    # provider (e.g. ORCID's sub is the ORCID iD itself, with no
    # preferred_username or email at all) -- sub is the one claim every
    # OIDC provider is required to return, so it's the final fallback.
    #
    # email ranks above name: it's a stable, meaningful identifier when a
    # provider shares it, versus name being a raw display string ("Kinshuk
    # S") with no uniqueness guarantee at all. Whichever of these four
    # wins still has to pass through sanitize_username() before storage --
    # none of them (including email) match validate_username()'s
    # ^[a-zA-Z0-9_]{3,63}$ rule on their own.
    username = (
        userinfo.get("preferred_username")
        or userinfo.get("email")
        or userinfo.get("name")
        or userinfo["sub"]
    )
    return {
        "username": username,
        "email": userinfo.get("email"),
        "auth_provider": provider,
        "external_sub_id": userinfo["sub"],
    }
