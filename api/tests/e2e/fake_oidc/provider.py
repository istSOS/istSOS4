# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License").
#
# Minimal fake OpenID Connect provider for end-to-end testing of the
# istSOS4 external-auth pathway. It issues REAL RS256-signed id_tokens and
# serves a JWKS, so Authlib inside the istSOS API validates them exactly
# as it would a real provider -- only the identity is canned.
#
# Endpoints:
#   GET  /.well-known/openid-configuration
#   GET  /authorize   -> browser: a consent screen; script: 302 straight back
#   POST /authorize   -> 302 back to redirect_uri with code + state
#   POST /token       -> {access_token, id_token, token_type}
#   GET  /userinfo    -> the claims (Bearer access_token)
#   GET  /jwks        -> public key
#
# PUBLIC vs INTERNAL base URL
# ---------------------------
# The browser and the API container reach this service by different names,
# exactly as a real IdP behind a reverse proxy does:
#
#   authorization_endpoint -> PUBLIC   (the browser is redirected here)
#   token_endpoint         -> INTERNAL (back-channel, API container only)
#   jwks_uri / userinfo    -> INTERNAL (back-channel)
#
# `issuer` uses PUBLIC and so does the id_token `iss` claim, so Authlib's
# issuer validation still matches.
#
# Env:
#   FAKE_OIDC_PUBLIC_BASE    (default "http://localhost:9000")
#   FAKE_OIDC_INTERNAL_BASE  (default "http://oidc-fake:9000")
#   FAKE_OIDC_SUB / _EMAIL / _NAME   defaults for the consent form

import html
import os
import time
import urllib.parse
import uuid

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

PUBLIC = os.getenv("FAKE_OIDC_PUBLIC_BASE", "http://localhost:9000").rstrip("/")
INTERNAL = os.getenv("FAKE_OIDC_INTERNAL_BASE", "http://oidc-fake:9000").rstrip("/")
ISSUER = PUBLIC

SUB = os.getenv("FAKE_OIDC_SUB", "fake-sub-001")
EMAIL = os.getenv("FAKE_OIDC_EMAIL", "oidctester@example.com")
NAME = os.getenv("FAKE_OIDC_NAME", "OIDC Tester")
KID = "fake-key-1"

# Demo personas offered as one-click buttons on the consent screen. Each is
# a distinct `sub`, so each produces a separate istSOS account that an
# administrator can activate with a different Network scope.
PERSONAS = [
    ("alice-sub-001", "alice@example.org", "Alice Researcher"),
    ("bob-sub-002", "bob@example.org", "Bob Fieldtech"),
    ("carol-sub-003", "carol@example.org", "Carol Analyst"),
]

_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_private_pem = _key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
)
_pub_numbers = _key.public_key().public_numbers()


def _b64u(n: int) -> str:
    import base64

    b = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


_JWK = {
    "kty": "RSA",
    "use": "sig",
    "alg": "RS256",
    "kid": KID,
    "n": _b64u(_pub_numbers.n),
    "e": _b64u(_pub_numbers.e),
}

# code -> {nonce, client_id, sub, email, name}
_CODES: dict[str, dict] = {}
# access_token -> claims
_TOKENS: dict[str, dict] = {}

app = FastAPI(title="fake-oidc-provider")


@app.get("/.well-known/openid-configuration")
def discovery():
    return {
        "issuer": ISSUER,
        # front-channel: the browser follows this one
        "authorization_endpoint": f"{PUBLIC}/authorize",
        # back-channel: only the API container calls these
        "token_endpoint": f"{INTERNAL}/token",
        "userinfo_endpoint": f"{INTERNAL}/userinfo",
        "jwks_uri": f"{INTERNAL}/jwks",
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "scopes_supported": ["openid", "email", "profile"],
        "claims_supported": ["sub", "email", "name"],
        "grant_types_supported": ["authorization_code"],
    }


@app.get("/jwks")
def jwks():
    return {"keys": [_JWK]}


def _issue_code(q, sub, email, name, redirect_uri, state):
    code = uuid.uuid4().hex
    _CODES[code] = {
        "nonce": q.get("nonce"),
        "client_id": q.get("client_id"),
        "sub": sub,
        "email": email,
        "name": name,
    }
    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        url=f"{redirect_uri}{sep}code={code}&state={urllib.parse.quote(state)}",
        status_code=302,
    )


_PAGE = """<!doctype html><meta charset="utf-8">
<title>Sign in — Demo Identity Provider</title>
<style>
 :root{{color-scheme:light}}
 body{{font:15px/1.5 system-ui,sans-serif;background:#eef1f6;margin:0;
      display:flex;min-height:100vh;align-items:center;justify-content:center}}
 .card{{background:#fff;padding:32px 34px;border-radius:12px;width:390px;
       box-shadow:0 2px 18px rgba(0,0,0,.13)}}
 h1{{font-size:19px;margin:0 0 4px}}
 .sub{{color:#667;font-size:13px;margin:0 0 20px}}
 .who{{background:#f5f7fa;border:1px solid #e2e6ee;border-radius:8px;
      padding:11px 13px;margin-bottom:16px;font-size:13px;color:#445}}
 .who b{{color:#111}}
 label{{display:block;font-size:12px;color:#556;margin:11px 0 4px;
       text-transform:uppercase;letter-spacing:.04em}}
 input{{width:100%;box-sizing:border-box;padding:9px 11px;font-size:14px;
       border:1px solid #ccd3e0;border-radius:6px;font-family:inherit}}
 button{{width:100%;margin-top:20px;padding:11px;font-size:15px;
        background:#2f6fed;color:#fff;border:0;border-radius:6px;
        cursor:pointer;font-family:inherit;font-weight:600}}
 button:hover{{background:#2559c4}}
 .quick{{margin-top:18px;border-top:1px solid #e6e9f0;padding-top:14px}}
 .quick p{{font-size:12px;color:#667;margin:0 0 8px;text-transform:uppercase;
          letter-spacing:.04em}}
 .quick a{{display:inline-block;margin:0 6px 6px 0;padding:5px 11px;
          font-size:13px;background:#f1f4f9;border:1px solid #dde2ec;
          border-radius:20px;color:#2f6fed;text-decoration:none}}
 .quick a:hover{{background:#e5eaf5}}
 .note{{margin-top:16px;font-size:11px;color:#8a91a0;line-height:1.5}}
</style>
<div class="card">
  <h1>Demo Identity Provider</h1>
  <p class="sub">Signing you in to <b>istSOS4</b></p>
  <div class="who">
    Client <b>{client_id}</b> is requesting scopes
    <b>{scope}</b>.<br>You will be returned to <b>{redirect_host}</b>.
  </div>
  <form method="post" action="/authorize">
    {hidden}
    <label>Subject (sub)</label>
    <input name="sub" value="{sub}" required>
    <label>Email</label>
    <input name="email" value="{email}" required>
    <label>Display name</label>
    <input name="name" value="{name}" required>
    <button type="submit">Sign in and continue</button>
  </form>
  <div class="quick">
    <p>Demo personas</p>
    {personas}
  </div>
  <p class="note">Test provider. It signs real RS256 id_tokens with a key
  generated at start-up and publishes the matching JWKS, so istSOS
  validates this login exactly as it would a production IdP.</p>
</div>
"""


@app.get("/authorize")
def authorize(request: Request):
    """Browser -> a consent screen. Script (Accept: */*) -> straight 302.

    The content-negotiation split keeps run_oidc_e2e.py working unchanged
    while giving a human something real to click through.
    """
    q = request.query_params
    redirect_uri = q["redirect_uri"]
    state = q.get("state", "")

    wants_html = "text/html" in request.headers.get("accept", "")
    if q.get("auto") == "1" or not wants_html:
        return _issue_code(q, SUB, EMAIL, NAME, redirect_uri, state)

    # Persona quick-links re-enter this same page with the fields prefilled.
    persona_links = ""
    for p_sub, p_email, p_name in PERSONAS:
        qs = urllib.parse.urlencode(
            {**dict(q), "sub": p_sub, "email": p_email, "name": p_name}
        )
        persona_links += (
            f'<a href="/authorize?{html.escape(qs)}">'
            f"{html.escape(p_name.split()[0])}</a>"
        )

    hidden = ""
    for key in ("redirect_uri", "state", "nonce", "client_id", "scope"):
        if q.get(key):
            hidden += (
                f'<input type="hidden" name="{key}" '
                f'value="{html.escape(q[key])}">'
            )

    return HTMLResponse(
        _PAGE.format(
            client_id=html.escape(q.get("client_id", "unknown")),
            scope=html.escape(q.get("scope", "openid")),
            redirect_host=html.escape(
                urllib.parse.urlparse(redirect_uri).netloc or redirect_uri
            ),
            hidden=hidden,
            sub=html.escape(q.get("sub", SUB)),
            email=html.escape(q.get("email", EMAIL)),
            name=html.escape(q.get("name", NAME)),
            personas=persona_links,
        )
    )


@app.post("/authorize")
def authorize_submit(
    redirect_uri: str = Form(...),
    state: str = Form(""),
    nonce: str = Form(None),
    client_id: str = Form(None),
    scope: str = Form(None),
    sub: str = Form(...),
    email: str = Form(...),
    name: str = Form(...),
):
    q = {"nonce": nonce, "client_id": client_id}
    return _issue_code(q, sub, email, name, redirect_uri, state)


@app.post("/token")
def token(
    grant_type: str = Form(...),
    code: str = Form(...),
    redirect_uri: str = Form(None),
    client_id: str = Form(None),
    client_secret: str = Form(None),
):
    entry = _CODES.pop(code, None)
    if entry is None:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    now = int(time.time())
    aud = client_id or entry.get("client_id") or "fake-client"
    sub = entry.get("sub") or SUB
    email = entry.get("email") or EMAIL
    name = entry.get("name") or NAME
    claims = {
        "iss": ISSUER,
        "sub": sub,
        "aud": aud,
        "exp": now + 3600,
        "iat": now,
        "email": email,
        "email_verified": True,
        "name": name,
        "preferred_username": None,
    }
    if entry.get("nonce"):
        claims["nonce"] = entry["nonce"]

    id_token = jwt.encode(
        claims, _private_pem, algorithm="RS256", headers={"kid": KID}
    )
    access_token = uuid.uuid4().hex
    _TOKENS[access_token] = {"sub": sub, "email": email, "name": name}
    return {
        "access_token": access_token,
        "id_token": id_token,
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "openid email profile",
    }


@app.get("/userinfo")
def userinfo(request: Request):
    auth = request.headers.get("authorization", "")
    tok = auth[7:] if auth.lower().startswith("bearer ") else ""
    info = _TOKENS.get(tok)
    if info is None:
        return JSONResponse({"error": "invalid_token"}, status_code=401)
    return {"sub": info["sub"], "email": info["email"], "name": info["name"]}
