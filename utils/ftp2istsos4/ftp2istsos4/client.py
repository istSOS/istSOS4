import json
import time
from datetime import datetime
from urllib.parse import quote

from .config import (
    observation_mode,
    overwrite_observations_enabled,
    require_value,
)
from .dependencies import import_requests
from .errors import DuplicateObservationError
from .observations import bulk_observations_payload


class DryRunResponse:
    status_code = 201


def parse_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def phenomenon_time_end(value):
    if not value:
        return None
    if isinstance(value, dict):
        value = value.get("end") or value.get("upper") or value.get("to")
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        value = value[1]
    if not isinstance(value, str):
        return parse_timestamp(value)
    if "/" in value:
        value = value.rsplit("/", 1)[1]
    return parse_timestamp(value)


def is_duplicate_observation_error(status_code, text):
    return (
        status_code == 400
        and "duplicate key value violates unique constraint" in text
        and "unique_observation_phenomenontime_datastreamid" in text
    ) or (
        status_code == 409
        and "Observation already exists" in text
    )


class IstsosClient:
    def __init__(
        self,
        base_url,
        username,
        password,
        dry_run=False,
        update=False,
        mode="append",
    ):
        self.requests = import_requests()
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.dry_run = dry_run
        self.update = update
        self.observation_mode = mode
        self.access_token = None
        self.refresh_token = None
        self.expires_at = 0
        self.datastreams_by_name = None
        self.datastreams_by_id = None

    def url(self, path):
        return f"{self.base_url}/{path.lstrip('/')}"

    def token_payload(self, grant_type, **extra):
        payload = {"grant_type": grant_type}
        payload.update(extra)
        return payload

    def read_token_response(self, response):
        response.raise_for_status()
        data = response.json()
        access_token = (
            data.get("access_token")
            or data.get("accessToken")
            or data.get("token")
        )
        if not access_token:
            raise ValueError("Token response did not include an access token")

        self.access_token = access_token
        self.refresh_token = (
            data.get("refresh_token")
            or data.get("refreshToken")
            or self.refresh_token
        )
        expires_in = data.get("expires_in") or data.get("expiresIn")
        if expires_in is not None:
            expires_in = int(expires_in)
            now = time.time()
            if expires_in > now:
                self.expires_at = expires_in - 30
            else:
                self.expires_at = now + max(expires_in - 30, 0)
        else:
            self.expires_at = 0

    def post_token_request(self, path, payload, headers=None):
        response = self.requests.post(
            self.url(path), data=payload, headers=headers, timeout=(5, 30)
        )
        if response.status_code in (400, 415, 422):
            response = self.requests.post(
                self.url(path),
                json=payload,
                headers=headers,
                timeout=(5, 30),
            )
        self.read_token_response(response)

    def login(self):
        payload = self.token_payload(
            "password", username=self.username, password=self.password
        )
        redacted_payload = dict(payload)
        redacted_payload["password"] = "<REDACTED>"
        print(f"POST {self.url('/Login')}")
        print(f"body: {redacted_payload}")
        self.post_token_request("/Login", payload)

    def refresh(self):
        if not self.access_token:
            self.login()
            return

        payload = self.token_payload("refresh_token")
        if self.refresh_token:
            payload["refresh_token"] = self.refresh_token

        try:
            self.post_token_request(
                "/Refresh", payload, headers=self.authorized_headers()
            )
        except self.requests.HTTPError:
            self.login()

    def ensure_token(self):
        if not self.access_token:
            self.login()
        elif self.expires_at and time.time() >= self.expires_at:
            self.refresh()

    def authorized_headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_json(self, path_or_url):
        if path_or_url.startswith(("http://", "https://")):
            url = path_or_url
        else:
            url = self.url(path_or_url)

        self.ensure_token()
        response = self.requests.get(
            url, headers=self.authorized_headers(), timeout=(5, 30)
        )
        if response.status_code == 401:
            self.refresh()
            response = self.requests.get(
                url, headers=self.authorized_headers(), timeout=(5, 30)
            )
        if response.status_code != 200:
            text = response.text[:500]
            raise RuntimeError(
                f"GET {url} failed: status={response.status_code}, body={text}"
            )
        return response.json()

    def fetch_datastreams(self):
        print(f"GET {self.url('/Datastreams')}", flush=True)
        datastreams = []
        next_page = "/Datastreams"

        while next_page:
            data = self.get_json(next_page)
            if isinstance(data, list):
                items = data
                next_page = None
            elif isinstance(data, dict):
                items = (
                    data.get("value")
                    or data.get("data")
                    or data.get("datastreams")
                    or []
                )
                next_page = (
                    data.get("@iot.nextLink")
                    or data.get("nextLink")
                    or data.get("next")
                )
            else:
                items = []
                next_page = None

            if not isinstance(items, list):
                raise RuntimeError("Datastreams response did not contain a list")
            datastreams.extend(items)

        by_name = {}
        by_id = {}
        duplicates = set()
        for datastream in datastreams:
            if not isinstance(datastream, dict):
                continue
            name = datastream.get("name")
            datastream_id = datastream.get("@iot.id")
            if name in (None, "") or datastream_id in (None, ""):
                continue
            if name in by_name:
                duplicates.add(name)
            by_name[name] = datastream_id
            by_id[datastream_id] = datastream

        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise RuntimeError(f"Duplicate datastream names in istSOS: {names}")

        self.datastreams_by_name = by_name
        self.datastreams_by_id = by_id
        print(f"Datastreams loaded: {len(by_name)}", flush=True)
        return by_name

    def ensure_datastreams(self):
        if self.datastreams_by_name is None:
            return self.fetch_datastreams()
        return self.datastreams_by_name

    def resolve_datastream_id(self, name):
        datastreams = self.ensure_datastreams()
        if name not in datastreams:
            print(
                f"Datastream '{name}' not found. Refreshing datastream list.",
                flush=True,
            )
            datastreams = self.fetch_datastreams()
        if name not in datastreams:
            available = ", ".join(sorted(datastreams)[:20])
            suffix = ""
            if len(datastreams) > 20:
                suffix = f", ... ({len(datastreams)} total)"
            raise ValueError(
                f"Datastream '{name}' does not exist in istSOS. "
                f"Available datastreams: {available}{suffix}"
            )
        return datastreams[name]

    def datastream_phenomenon_time_end(self, datastream_id):
        self.ensure_datastreams()
        datastream = (self.datastreams_by_id or {}).get(datastream_id)
        if not datastream:
            return None
        return phenomenon_time_end(datastream.get("phenomenonTime"))

    def record_observation_times(self, observations):
        self.ensure_datastreams()
        if self.datastreams_by_id is None:
            return
        for observation in observations:
            datastream_id = observation["Datastream"]["@iot.id"]
            observed = parse_timestamp(observation.get("phenomenonTime"))
            if observed is None:
                continue
            datastream = self.datastreams_by_id.get(datastream_id)
            if datastream is None:
                continue
            current = phenomenon_time_end(datastream.get("phenomenonTime"))
            if current is None or observed > current:
                datastream["phenomenonTime"] = (
                    f"{datastream.get('phenomenonTime', '').split('/')[0]}"
                    f"/{observation['phenomenonTime']}"
                    if datastream.get("phenomenonTime")
                    else observation["phenomenonTime"]
                )

    def observation_filter(self, datastream_id, start, end):
        parts = [f"Datastream/@iot.id eq {datastream_id}"]
        if start:
            parts.append(f"phenomenonTime ge {start}")
        if end:
            parts.append(f"phenomenonTime le {end}")
        return " and ".join(parts)

    def fetch_observations(self, datastream_id, start, end):
        query_filter = quote(
            self.observation_filter(datastream_id, start, end), safe=":/@."
        )
        select = quote("@iot.id,phenomenonTime,result,resultQuality", safe="@.,")
        next_page = (
            f"/Observations?$filter={query_filter}"
            f"&$select={select}&$top=1000"
        )
        observations = []
        while next_page:
            url = (
                next_page
                if next_page.startswith(("http://", "https://"))
                else self.url(next_page)
            )
            print(f"GET {url}", flush=True)
            data = self.get_json(next_page)
            if isinstance(data, list):
                items = data
                next_page = None
            elif isinstance(data, dict):
                items = (
                    data.get("value")
                    or data.get("data")
                    or data.get("observations")
                    or []
                )
                next_page = (
                    data.get("@iot.nextLink")
                    or data.get("nextLink")
                    or data.get("next")
                )
            else:
                items = []
                next_page = None
            if not isinstance(items, list):
                raise RuntimeError("Observations response did not contain a list")
            observations.extend(items)
        return observations

    def post_observation(self, observation):
        print(f"POST {self.url('/Observations')}", flush=True)
        if self.dry_run:
            print(
                json.dumps(observation, ensure_ascii=False, indent=2),
                flush=True,
            )
            print("DRY RUN: POST disabled", flush=True)
            return DryRunResponse()

        self.ensure_token()
        response = self.requests.post(
            self.url("/Observations"),
            json=observation,
            headers=self.authorized_headers(),
            timeout=(5, 30),
        )
        if response.status_code == 401:
            self.refresh()
            response = self.requests.post(
                self.url("/Observations"),
                json=observation,
                headers=self.authorized_headers(),
                timeout=(5, 30),
            )
        if response.status_code != 201:
            text = response.text[:500]
            if is_duplicate_observation_error(response.status_code, text):
                raise DuplicateObservationError(
                    "Observation already exists: "
                    f"status={response.status_code}, body={text}"
                )
            raise RuntimeError(
                "Observation POST failed: "
                f"status={response.status_code}, body={text}"
            )
        return response

    def patch_observation(self, observation_id, observation):
        path = f"/Observations({observation_id})"
        payload = {
            "result": observation["result"],
            "resultQuality": observation["resultQuality"],
        }
        print(f"PATCH {self.url(path)}", flush=True)
        if self.dry_run:
            print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
            print("DRY RUN: PATCH disabled", flush=True)
            return DryRunResponse()

        self.ensure_token()
        response = self.requests.patch(
            self.url(path),
            json=payload,
            headers=self.authorized_headers(),
            timeout=(5, 30),
        )
        if response.status_code == 401:
            self.refresh()
            response = self.requests.patch(
                self.url(path),
                json=payload,
                headers=self.authorized_headers(),
                timeout=(5, 30),
            )
        if response.status_code not in (200, 204):
            text = response.text[:500]
            raise RuntimeError(
                "Observation PATCH failed: "
                f"status={response.status_code}, body={text}"
            )
        return response

    def post_bulk_observations(self, observations):
        payload = bulk_observations_payload(observations)
        print(f"POST {self.url('/BulkObservations')}", flush=True)
        if self.dry_run:
            print(
                json.dumps(payload, ensure_ascii=False, indent=2),
                flush=True,
            )
            print("DRY RUN: POST disabled", flush=True)
            return DryRunResponse()

        self.ensure_token()
        response = self.requests.post(
            self.url("/BulkObservations"),
            json=payload,
            headers=self.authorized_headers(),
            timeout=(5, 30),
        )
        if response.status_code == 401:
            self.refresh()
            response = self.requests.post(
                self.url("/BulkObservations"),
                json=payload,
                headers=self.authorized_headers(),
                timeout=(5, 30),
            )
        if response.status_code not in (200, 201, 202):
            text = response.text[:500]
            if is_duplicate_observation_error(response.status_code, text):
                raise DuplicateObservationError(
                    "BulkObservations contains existing observations: "
                    f"status={response.status_code}, body={text}"
                )
            raise RuntimeError(
                "BulkObservations POST failed: "
                f"status={response.status_code}, body={text}"
            )
        return response


def build_istsos_client(config):
    url = require_value(config, "istsos_url")
    username = require_value(config, "istsos_username")
    password = require_value(config, "istsos_password")
    dry_run = bool(config.get("dry_run", False))
    update = overwrite_observations_enabled(config)
    mode = observation_mode(config)
    return IstsosClient(
        url, username, password, dry_run=dry_run, update=update, mode=mode
    )
