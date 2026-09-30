#!/usr/bin/env python3
# Copyright 2025 SUPSI
#
# NARRATED walk-through of the istSOS4 external (OIDC) auth pathway, for a
# live demo. Same flow as run_oidc_e2e.py but it stops to SHOW the
# evidence at each step so an audience can see it is a real OpenID Connect
# handshake -- not a mock.
#
#   python api/tests/e2e/demo_oidc.py            # runs straight through
#   python api/tests/e2e/demo_oidc.py --pause    # waits for Enter between steps
#
# Prereq:
#   docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --build

import base64
import json
import subprocess
import sys
import urllib.parse

import requests

BASE = "http://localhost:8018/istsos4/v1.1"
FAKE = "http://localhost:9000"
PAUSE = "--pause" in sys.argv


def banner(t):
    print("\n" + "=" * 72 + f"\n  {t}\n" + "=" * 72)


def pause():
    if PAUSE:
        input("\n   ...press Enter...\n")


def psql(sql):
    return subprocess.run(
        ["docker", "exec", "istsos4-database", "psql", "-U", "postgres",
         "-d", "istsos", "-tAc", sql],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def reset_oidc_users():
    """Remove any OIDC-provisioned users so the demo starts from a clean
    'brand new external identity' state. Rerunnable. The AuditLog is
    append-only, so its ON DELETE SET NULL FK action (which runs as the
    'administrator' table owner) needs a momentary UPDATE grant."""
    psql(
        'GRANT UPDATE ON sensorthings."AuditLog" TO "administrator";'
        'DELETE FROM sensorthings."User" WHERE auth_provider IS NOT NULL;'
        'REVOKE UPDATE ON sensorthings."AuditLog" FROM "administrator";'
    )


def fake_log_lines():
    return subprocess.run(
        ["docker", "logs", "istsos4-oidc-fake"],
        capture_output=True, text=True,
    ).stderr.splitlines() + subprocess.run(
        ["docker", "logs", "istsos4-oidc-fake"],
        capture_output=True, text=True,
    ).stdout.splitlines()


def jwt_parts(tok):
    h, p, _sig = tok.split(".")
    pad = lambda s: s + "=" * (-len(s) % 4)
    return (json.loads(base64.urlsafe_b64decode(pad(h))),
            json.loads(base64.urlsafe_b64decode(pad(p))))


def roundtrip(sess, **params):
    r1 = sess.get(f"{BASE}/auth/google/login", params=params,
                  allow_redirects=False)
    loc = r1.headers["location"]
    print(f"   API 302 -> {loc[:90]}...")
    au = loc.replace("http://oidc-fake:9000", FAKE)
    r2 = sess.get(au, allow_redirects=False)
    cb = r2.headers["location"]
    print(f"   provider 302 -> ...{cb.split('?')[0].rsplit('/',1)[-1]}?{cb.split('?')[1][:60]}...")
    parsed = urllib.parse.urlparse(cb)
    cb = cb.replace(f"{parsed.scheme}://{parsed.netloc}",
                    BASE.rsplit("/istsos4", 1)[0])
    return sess.get(cb, allow_redirects=False)


# ---------------------------------------------------------------------------
reset_oidc_users()
# cold-start the API so Authlib re-fetches discovery + JWKS during the
# demo (proves it, rather than serving a warm cache).
subprocess.run(["docker", "restart", "istsos4-api"],
               capture_output=True, check=True)
for _ in range(30):
    try:
        if requests.get(f"{BASE}/", timeout=2).status_code:
            break
    except Exception:  # noqa: BLE001
        pass
    __import__("time").sleep(1)

banner("0.  The identity provider is a REAL HTTP server")
disc = requests.get(f"{FAKE}/.well-known/openid-configuration").json()
print("   GET /.well-known/openid-configuration :")
for k in ("issuer", "authorization_endpoint", "token_endpoint", "jwks_uri"):
    print(f"      {k:24} {disc[k]}")
jwks = requests.get(f"{FAKE}/jwks").json()
print(f"   GET /jwks : 1 RSA public key, kid={jwks['keys'][0]['kid']}, alg={jwks['keys'][0]['alg']}")
print("   -> This is what Google/Microsoft/ORCID publish too. istSOS's")
print("      Authlib client will fetch these and validate against them.")
pause()

# ---------------------------------------------------------------------------
banner("1.  A new user signs in with the external provider")
net_id, net_name = psql(
    'SELECT id, name FROM sensorthings."Network" ORDER BY id LIMIT 1').split("|")
ds_in_net = int(psql(
    f'SELECT count(*) FROM sensorthings."Datastream" WHERE network_id={net_id}'))
ds_total = int(psql('SELECT count(*) FROM sensorthings."Datastream"'))
print(f"   They request scoped access to Network '{net_name}'.")
print(f"   (ground truth: that network has {ds_in_net} of {ds_total} datastreams)")

before = len(fake_log_lines())
s1 = requests.Session()
r = roundtrip(s1, dataset_id=net_name, requested_role="viewer")
print(f"\n   istSOS /callback -> HTTP {r.status_code}: {r.json().get('message')}")
pause()

# ---------------------------------------------------------------------------
banner("2.  PROOF the API did a real server-to-server code exchange")
new = fake_log_lines()[before:]
for ln in new:
    if any(x in ln for x in ("/token", "/jwks", "well-known", "/authorize")):
        who = "BROWSER  " if "172.22.0.1:" in ln else "istSOS API"
        req = ln.split('"')[1] if '"' in ln else ln
        print(f"   [{who}]  {req[:78]}")
print("\n   Two different clients hit the provider:")
print("     - the BROWSER (this script) -> GET /authorize")
print("     - the istSOS API container  -> POST /token  +  GET /jwks")
print("   The API exchanged the one-time code for tokens and fetched the")
print("   signing keys itself. Authlib does this; we don't touch it.")
pause()

# ---------------------------------------------------------------------------
banner("3.  The id_token the provider signed (a real RS256 JWT)")
# mint one directly so we can show + verify it
code_sess = requests.Session()
r1 = code_sess.get(f"{BASE}/auth/google/login", allow_redirects=False)
au = r1.headers["location"].replace("http://oidc-fake:9000", FAKE)
r2 = code_sess.get(au, allow_redirects=False)
code = urllib.parse.parse_qs(urllib.parse.urlparse(r2.headers["location"]).query)["code"][0]
tokresp = requests.post(f"{FAKE}/token", data={
    "grant_type": "authorization_code", "code": code,
    "client_id": "fake-client", "client_secret": "fake-secret",
    "redirect_uri": f"{BASE}/auth/google/callback"}).json()
idt = tokresp["id_token"]
hdr, pl = jwt_parts(idt)
print(f"   header : {json.dumps(hdr)}")
print("   claims :")
for k in ("iss", "sub", "aud", "email", "name", "nonce", "exp"):
    if k in pl:
        print(f"      {k:8} {pl[k]}")
print("\n   Verifying the signature against the provider's JWKS "
      "(inside the API's own container):")
verify = subprocess.run(["docker", "exec", "istsos4-api", "python", "-c", f'''
import jwt, json, urllib.request
jwks = json.load(urllib.request.urlopen("http://oidc-fake:9000/jwks"))
key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwks["keys"][0]))
c = jwt.decode("{idt}", key=key, algorithms=["RS256"], audience="fake-client")
print("   OK  signature valid, sub =", c["sub"])
tampered = "{idt}"[:-4] + ("AAAA" if not "{idt}".endswith("AAAA") else "BBBB")
try:
    jwt.decode(tampered, key=key, algorithms=["RS256"], audience="fake-client")
    print("   ?? tampered token accepted -- BUG")
except Exception as e:
    print("   OK  one flipped byte ->", type(e).__name__)
'''], capture_output=True, text=True)
print(verify.stdout or verify.stderr)
print("   This is exactly the check Authlib runs inside /callback. If it")
print("   failed, the callback would return 400 -- it returned 202/200.")
pause()

# ---------------------------------------------------------------------------
banner("4.  Admin approves the pending account, scoped to the Network")
tok = requests.post(f"{BASE}/Login",
                    data={"username": "admin", "password": "admin"}).json()["access_token"]
uid = psql("SELECT id FROM sensorthings.\"User\" "
           "WHERE auth_provider='google' AND external_sub_id='fake-sub-001'")
row_before = psql(f'SELECT role, dataset_id FROM sensorthings."User" WHERE id={uid}')
print(f"   before:  User {uid}  role|dataset = {row_before}")
a = requests.patch(f"{BASE}/Users/{uid}/policy-approval",
                  headers={"Authorization": f"Bearer {tok}"},
                  json={"role": "viewer", "dataset": net_name})
print(f"   PATCH /Users/{uid}/policy-approval {{role:viewer, dataset:{net_name}}} -> {a.status_code}: {a.json().get('message')}")
row_after = psql(f'SELECT role, dataset_id FROM sensorthings."User" WHERE id={uid}')
print(f"   after:   User {uid}  role|dataset = {row_after}")
audit = psql("SELECT action_type, dataset_id FROM sensorthings.\"AuditLog\" "
             f"WHERE (payload->>'approved_user_id')::bigint = {uid}")
print(f"   audit log row: {audit}")
pause()

# ---------------------------------------------------------------------------
banner("5.  The user logs in via the provider again -> a real istSOS JWT")
s3 = requests.Session()
r = roundtrip(s3)
jwt_tok = r.json()["access_token"]
h, p = jwt_parts(jwt_tok)
print(f"\n   /callback -> HTTP {r.status_code}, access_token issued by istSOS:")
print(f"      header {json.dumps(h)}")
print(f"      claims {json.dumps(p)}")
print("   sub is the username istSOS derived + sanitised from the OIDC")
print("   email; role is what the admin granted. Same JWT shape /Login gives.")
pause()

# ---------------------------------------------------------------------------
banner("6.  Scoping is enforced by PostgreSQL, not the application")
pol = psql("SELECT qual FROM pg_policies WHERE tablename='Datastream' "
           "AND policyname='rbac_user_select_datastream'")
print("   The RLS policy on Datastream for the 'user' group role:")
print(f"      USING {pol}")
print("\n   Run the SAME query the OIDC user's request runs, straight in psql")
print("   (impersonating their DB session: SET LOCAL ROLE user + their id):")
direct = psql(
    f"SET LOCAL ROLE \"user\"; "
    f"SELECT set_config('app.current_user_id','{uid}',true); "
    f"SELECT count(*) FROM sensorthings.\"Datastream\";"
).splitlines()[-1]
print(f"      -> {direct} rows  (no application code involved)")
pause()

# ---------------------------------------------------------------------------
banner("7.  Three independent counts agree")
h = {"Authorization": f"Bearer {jwt_tok}"}
api_ds = requests.get(f"{BASE}/Datastreams",
                      params={"$count": "true", "$top": 1}, headers=h).json()["@iot.count"]
api_obs = requests.get(f"{BASE}/Observations",
                       params={"$count": "true", "$top": 1}, headers=h).json()["@iot.count"]
gt_ds = psql(f'SELECT count(*) FROM sensorthings."Datastream" WHERE network_id={net_id}')
gt_obs = psql('SELECT count(*) FROM sensorthings."Observation" o JOIN '
              f'sensorthings."Datastream" d ON d.id=o.datastream_id WHERE d.network_id={net_id}')
admin_ds = requests.get(f"{BASE}/Datastreams", params={"$count": "true", "$top": 1},
                        headers={"Authorization": f"Bearer {tok}"}).json()["@iot.count"]
print(f"   OIDC user via API          GET /Datastreams  $count = {api_ds}")
print(f"   direct psql (their session)                          = {direct}")
print(f"   ground truth  WHERE network_id = {net_id}                  = {gt_ds}")
print(f"   admin (unrestricted)       GET /Datastreams  $count = {admin_ds}   (all of them)")
print()
print(f"   OIDC user via API          GET /Observations $count = {api_obs}")
print(f"   ground truth (join on network)                       = {gt_obs}")
print()
nets = requests.get(f"{BASE}/Datastreams",
                    params={"$top": 100, "$expand": "Network($select=name)"},
                    headers=h).json()["value"]
seen = {x["Network"]["name"] for x in nets}
print(f"   every datastream the OIDC user can see is in: {seen}")
w = requests.patch(f"{BASE}/Datastreams({nets[0]['@iot.id']})",
                   headers={**h, "commit-message": "demo"}, json={"name": "x"})
print(f"   OIDC viewer tries to WRITE -> HTTP {w.status_code}  (blocked)")

ok = (str(api_ds) == direct == gt_ds and str(api_obs) == gt_obs
      and seen == {net_name} and w.status_code >= 400
      and str(admin_ds) == str(ds_total))
banner("PASS -- external identity, scoped to a Network, enforced in the DB"
       if ok else "MISMATCH -- see above")
sys.exit(0 if ok else 1)
