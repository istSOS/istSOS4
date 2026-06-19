import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


# -------------------------
# Istsos API Client (httpx)
# -------------------------
class IstsosClient:
    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        *,
        timeout_sec: int = 15,
        pool_size: int = 64,
        debug: bool = False,
    ):
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = httpx.Timeout(timeout_sec)
        self.debug = debug

        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

        limits = httpx.Limits(
            max_connections=pool_size,
            max_keepalive_connections=pool_size,
            keepalive_expiry=30.0,
        )

        self.client = httpx.Client(
            timeout=self.timeout,
            limits=limits,
            headers={"User-Agent": "istsos4-flink-client/1.0"},
        )

        # retry policy
        self._retry_statuses = {429, 502, 503, 504}
        self._max_retries = 3
        self._backoff_base = 0.2

    def close(self):
        self.client.close()

    # -------- auth ----------
    def _login(self, refresh: bool = False) -> str:
        if refresh:
            logger.info("Refreshing API token...")
            resp = self.client.post(
                f"{self.url}/Refresh",
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
            )
        else:
            resp = self.client.post(
                f"{self.url}/Login",
                data={
                    "username": self.username,
                    "password": self.password,
                    "grant_type": "password",
                },
            )

        if resp.status_code >= 400:
            logger.error("Auth failed [%s]: %s", resp.status_code, resp.text)
        resp.raise_for_status()

        token_data = resp.json()
        token = token_data.get("access_token")
        if not token:
            raise RuntimeError("Missing access_token in auth response")

        expires_in = token_data.get("expires_in")
        # expires_in è tipicamente "secondi"
        if expires_in is None:
            # fallback safe: 1h
            expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        else:
            expiry = datetime.now(timezone.utc) + timedelta(
                seconds=int(expires_in)
            )

        self._token = token
        self._token_expiry = expiry
        return token

    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._token and self._token_expiry:
            if self._token_expiry - now < timedelta(minutes=5):
                return self._login(refresh=True)
            return self._token
        return self._login(refresh=False)

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # -------- helpers ----------
    @staticmethod
    def _escape_odata_literal(val: str) -> str:
        return val.replace("'", "''")

    # -------- request wrapper (retry + refresh su 401) ----------
    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        # Se arrivano URL completi (es. @iot.nextLink), usali.
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        else:
            url = (
                f"{self.url}{path}"
                if path.startswith("/")
                else f"{self.url}/{path}"
            )

        headers = kwargs.pop("headers", {})
        merged = {**self._auth_headers(), **headers}

        last_exc: Optional[Exception] = None

        for attempt in range(self._max_retries + 1):
            try:
                resp = self.client.request(
                    method, url, headers=merged, **kwargs
                )

                # 409: nel tuo codice la lasci passare senza retry
                if resp.status_code == 409:
                    resp.raise_for_status()
                    return resp

                # 401: refresh token e retry UNA volta (sul tentativo corrente)
                if resp.status_code == 401:
                    logger.warning(
                        "401 received, refreshing token and retrying once..."
                    )
                    self._login(refresh=False)
                    merged = {**self._auth_headers(), **headers}
                    resp = self.client.request(
                        method, url, headers=merged, **kwargs
                    )

                # retry su status forcelist
                if (
                    resp.status_code in self._retry_statuses
                    and attempt < self._max_retries
                ):
                    delay = self._backoff_base * (2**attempt)
                    logger.warning(
                        "Retryable status %s on %s %s. Retrying in %.2fs (attempt %d/%d)",
                        resp.status_code,
                        method,
                        url,
                        delay,
                        attempt + 1,
                        self._max_retries,
                    )
                    time.sleep(delay)
                    continue

                if resp.status_code >= 400:
                    logger.error(
                        "%s %s failed [%s]\nResponse:\n%s",
                        method,
                        url,
                        resp.status_code,
                        resp.text,
                    )

                resp.raise_for_status()
                return resp

            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                if attempt < self._max_retries:
                    delay = self._backoff_base * (2**attempt)
                    logger.warning(
                        "Transport error on %s %s: %s. Retrying in %.2fs (attempt %d/%d)",
                        method,
                        url,
                        e,
                        delay,
                        attempt + 1,
                        self._max_retries,
                    )
                    time.sleep(delay)
                    continue
                logger.exception(
                    "Transport error on %s %s (no retries left): %s",
                    method,
                    url,
                    e,
                )
                raise

            except httpx.HTTPStatusError as e:
                # già loggato sopra prima di raise_for_status(), ma teniamo body
                body = e.response.text if e.response is not None else ""
                logger.error(
                    "HTTPStatusError on %s %s: %s\n%s", method, url, e, body
                )
                raise

        # Se arrivi qui è un caso limite (dovresti aver già raise)
        if last_exc:
            raise last_exc
        raise RuntimeError("Unexpected request failure")

    # -------------------------
    # API methods
    # -------------------------
    def get_datastream(self, name_or_id: str) -> dict | None:
        if str(name_or_id).isdigit():
            params = {"$filter": f"id eq '{name_or_id}'"}
        else:
            safe = self._escape_odata_literal(name_or_id)
            params = {"$filter": f"name eq '{safe}'"}

        resp = self._request("GET", "/Datastreams", params=params)
        vals = resp.json().get("value", [])
        if not vals:
            logger.error("Datastream not found: %s", name_or_id)
            return None
        return vals[0]

    def get_datastream_id(self, name_or_id: str) -> int | None:
        if str(name_or_id).isdigit():
            return int(name_or_id)

        safe = self._escape_odata_literal(name_or_id)
        params = {
            "$filter": f"name eq '{safe}'",
            "$select": "@iot.id",
            "$top": 1,
        }
        resp = self._request("GET", "/Datastreams", params=params)
        vals = resp.json().get("value", [])
        if not vals:
            logger.error("Datastream not found by name: %s", name_or_id)
            return None
        ds = vals[0]
        datastream_id = ds.get("@iot.id") or ds.get("id")
        return int(datastream_id) if datastream_id is not None else None

    def get_observation(
        self, obs: dict[str, Any], ds_name: str
    ) -> httpx.Response:
        pheno_time = obs.get("phenomenonTime")
        safe = self._escape_odata_literal(ds_name)
        params = {
            "$filter": f"phenomenonTime eq '{pheno_time}' and Datastream/name eq '{safe}'",
            "$select": "@iot.id",
            "$top": 1,
        }
        return self._request("GET", "/Observations", params=params)

    def insert_observation(
        self,
        obs: dict[str, Any],
        ds_name: str,
        commit_message: Optional[str] = None,
    ) -> httpx.Response:
        if self.debug:
            logger.info(
                "About to POST Observation: %s for %s",
                obs.get("phenomenonTime"),
                ds_name,
            )
        if commit_message:
            headers = {"commit-message": commit_message}
            return self._request(
                "POST", "/Observations", json=obs, headers=headers
            )
        else:
            return self._request("POST", "/Observations", json=obs)

    def bulk_observations(
        self, ds_id: str, obs: list[list[Any]]
    ) -> httpx.Response:
        if self.debug:
            logger.info(
                "About to POST BulkObservations: %d observations", len(obs)
            )
        payload = [
            {
                "Datastream": {"@iot.id": ds_id},
                "components": [
                    "result",
                    "phenomenonTime",
                    "resultTime",
                    "resultQuality",
                ],
                "dataArray": obs,
            }
        ]
        return self._request("POST", "/BulkObservations", json=payload)

    def get_datastreams(self, filter: str | None = None) -> list[dict]:
        params = {"$select": "@iot.id,name,properties"}
        resp = self._request(
            "GET",
            f"/Datastreams?$filter={filter}" if filter else "/Datastreams",
            params=params,
        )
        data = resp.json()
        v = data.get("value", [])
        while "@iot.nextLink" in data:
            logger.info("get_datastreams: paginated, fetching next page...")
            resp = self._request("GET", data["@iot.nextLink"])
            data = resp.json()
            v.extend(data.get("value", []))
        return v

    def get_observations(
        self, datastream_id: str, *, filter: str | None = None
    ) -> list[dict]:
        path = (
            f"/Datastreams({datastream_id})/Observations?$filter={filter}"
            if filter
            else f"/Datastreams({datastream_id})/Observations"
        )
        resp = self._request("GET", path)
        data = resp.json()
        v = data.get("value", [])
        while "@iot.nextLink" in data:
            logger.info("get_observations: paginated, fetching next page...")
            resp = self._request("GET", data["@iot.nextLink"])
            data = resp.json()
            v.extend(data.get("value", []))
        return v

    def patch_observation(
        self, observation_id: str, obs_patch: dict[str, Any]
    ) -> httpx.Response:
        merged = {**self._auth_headers(), "commit-message": "QC update"}
        return self._request(
            "PATCH",
            f"/Observations({observation_id})",
            json=obs_patch,
            headers=merged,
        )


# -------------------------
# Istsos API Client (ASYNC)
# -------------------------
class IstsosAsyncClient:
    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        *,
        timeout_sec: int = 15,
        pool_size: int = 64,
        debug: bool = False,
    ):
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.debug = debug

        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._auth_lock = asyncio.Lock()

        self._retry_statuses = {429, 502, 503, 504}
        self._max_retries = 3
        self._backoff_base = 0.2

        limits = httpx.Limits(
            max_connections=pool_size,
            max_keepalive_connections=pool_size,
            keepalive_expiry=30.0,
        )

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec),
            limits=limits,
            headers={"User-Agent": "istsos4-fastapi-client/1.0"},
            # http2=True,  # opzionale
        )

    async def aclose(self):
        await self.client.aclose()

    # -------- auth ----------
    async def _login(self, refresh: bool = False) -> str:
        if refresh:
            logger.info("Refreshing API token...")
            resp = await self.client.post(
                f"{self.url}/Refresh",
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
            )
        else:
            resp = await self.client.post(
                f"{self.url}/Login",
                data={
                    "username": self.username,
                    "password": self.password,
                    "grant_type": "password",
                },
            )

        if resp.status_code >= 400:
            logger.error("Auth failed [%s]: %s", resp.status_code, resp.text)
        resp.raise_for_status()

        token_data = resp.json()
        token = token_data.get("access_token")
        if not token:
            raise RuntimeError("Missing access_token in auth response")

        expires_in = token_data.get("expires_in")
        if expires_in is None:
            expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        else:
            expiry = datetime.now(timezone.utc) + timedelta(
                seconds=int(expires_in)
            )

        self._token = token
        self._token_expiry = expiry
        return token

    async def _get_token(self) -> str:
        # Evita 20 login paralleli sotto carico
        async with self._auth_lock:
            now = datetime.now(timezone.utc)
            if self._token and self._token_expiry:
                if self._token_expiry - now < timedelta(minutes=5):
                    return await self._login(refresh=True)
                return self._token
            return await self._login(refresh=False)

    async def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {await self._get_token()}",
            "Content-Type": "application/json",
        }

    # -------- helpers ----------
    @staticmethod
    def _escape_odata_literal(val: str) -> str:
        return val.replace("'", "''")

    # -------- request wrapper ----------
    async def _request(
        self, method: str, path: str, **kwargs
    ) -> httpx.Response:
        # supporta URL assoluti (nextLink)
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        else:
            url = (
                f"{self.url}{path}"
                if path.startswith("/")
                else f"{self.url}/{path}"
            )

        headers = kwargs.pop("headers", {})
        merged = {**(await self._auth_headers()), **headers}

        for attempt in range(self._max_retries + 1):
            try:
                resp = await self.client.request(
                    method, url, headers=merged, **kwargs
                )

                # 409: lascia passare
                if resp.status_code == 409:
                    resp.raise_for_status()
                    return resp

                # 401: refresh token + retry una volta
                if resp.status_code == 401:
                    logger.warning(
                        "401 received, refreshing token and retrying once..."
                    )
                    async with self._auth_lock:
                        # forza nuovo login
                        self._token = None
                        self._token_expiry = None
                    merged = {**(await self._auth_headers()), **headers}
                    resp = await self.client.request(
                        method, url, headers=merged, **kwargs
                    )

                # retry su status forcelist
                if (
                    resp.status_code in self._retry_statuses
                    and attempt < self._max_retries
                ):
                    delay = self._backoff_base * (2**attempt)
                    logger.warning(
                        "Retryable status %s on %s %s. Retrying in %.2fs (attempt %d/%d)",
                        resp.status_code,
                        method,
                        url,
                        delay,
                        attempt + 1,
                        self._max_retries,
                    )
                    await asyncio.sleep(delay)
                    continue

                if resp.status_code >= 400:
                    logger.error(
                        "%s %s failed [%s]\nResponse:\n%s",
                        method,
                        url,
                        resp.status_code,
                        resp.text,
                    )

                resp.raise_for_status()
                return resp

            except (httpx.TimeoutException, httpx.TransportError) as e:
                if attempt < self._max_retries:
                    delay = self._backoff_base * (2**attempt)
                    logger.warning(
                        "Transport error on %s %s: %s. Retrying in %.2fs (attempt %d/%d)",
                        method,
                        url,
                        e,
                        delay,
                        attempt + 1,
                        self._max_retries,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.exception(
                    "Transport error on %s %s (no retries left): %s",
                    method,
                    url,
                    e,
                )
                raise

            except httpx.HTTPStatusError as e:
                body = e.response.text if e.response is not None else ""
                logger.error(
                    "HTTPStatusError on %s %s: %s\n%s", method, url, e, body
                )
                raise

        raise RuntimeError("Unexpected request failure")

    # -------------------------
    # API methods
    # -------------------------
    async def get_datastream(self, name_or_id: str) -> dict | None:
        if str(name_or_id).isdigit():
            params = {"$filter": f"id eq '{name_or_id}'"}
        else:
            safe = self._escape_odata_literal(name_or_id)
            params = {"$filter": f"name eq '{safe}'"}

        resp = await self._request("GET", "/Datastreams", params=params)
        vals = resp.json().get("value", [])
        if not vals:
            logger.error("Datastream not found: %s", name_or_id)
            return None
        return vals[0]

    async def get_datastream_id(self, name_or_id: str) -> int | None:
        if str(name_or_id).isdigit():
            return int(name_or_id)

        safe = self._escape_odata_literal(name_or_id)
        params = {
            "$filter": f"name eq '{safe}'",
            "$select": "@iot.id",
            "$top": 1,
        }
        resp = await self._request("GET", "/Datastreams", params=params)
        vals = resp.json().get("value", [])
        if not vals:
            logger.error("Datastream not found by name: %s", name_or_id)
            return None
        ds = vals[0]
        datastream_id = ds.get("@iot.id") or ds.get("id")
        return int(datastream_id) if datastream_id is not None else None

    async def get_observation(
        self, obs: Dict[str, Any], ds_name: str
    ) -> httpx.Response:
        pheno_time = obs.get("phenomenonTime")
        safe = self._escape_odata_literal(ds_name)
        params = {
            "$filter": f"phenomenonTime eq '{pheno_time}' and Datastream/name eq '{safe}'",
            "$select": "@iot.id",
            "$top": 1,
        }
        return await self._request("GET", "/Observations", params=params)

    async def insert_observation(
        self,
        obs: Dict[str, Any],
        ds_name: str,
        commit_message: Optional[str] = None,
    ) -> httpx.Response:
        if self.debug:
            logger.info(
                "About to POST Observation: %s for %s",
                obs.get("phenomenonTime"),
                ds_name,
            )
        if commit_message:
            headers = {"commit-message": commit_message}
            return await self._request(
                "POST", "/Observations", json=obs, headers=headers
            )
        else:
            return await self._request("POST", "/Observations", json=obs)

    async def bulk_observations(
        self, ds_id: str, obs: List[List[Any]]
    ) -> httpx.Response:
        if self.debug:
            logger.info(
                "About to POST BulkObservations: %d observations", len(obs)
            )
        payload = [
            {
                "Datastream": {"@iot.id": ds_id},
                "components": [
                    "result",
                    "phenomenonTime",
                    "resultTime",
                    "resultQuality",
                ],
                "dataArray": obs,
            }
        ]
        return await self._request("POST", "/BulkObservations", json=payload)

    async def get_datastreams(self, filter: str | None = None) -> list[dict]:
        params = {"$select": "@iot.id,name,properties"}
        resp = await self._request(
            "GET",
            f"/Datastreams?$filter={filter}" if filter else "/Datastreams",
            params=params,
        )
        data = resp.json()
        v = data.get("value", [])
        while "@iot.nextLink" in data:
            logger.info("get_datastreams: paginated, fetching next page...")
            resp = await self._request("GET", data["@iot.nextLink"])
            data = resp.json()
            v.extend(data.get("value", []))
        return v

    async def get_observations(
        self, datastream_id: str, *, filter: str | None = None
    ) -> list[dict]:
        path = (
            f"/Datastreams({datastream_id})/Observations?$filter={filter}"
            if filter
            else f"/Datastreams({datastream_id})/Observations"
        )
        resp = await self._request("GET", path)
        data = resp.json()
        v = data.get("value", [])
        while "@iot.nextLink" in data:
            logger.info("get_observations: paginated, fetching next page...")
            resp = await self._request("GET", data["@iot.nextLink"])
            data = resp.json()
            v.extend(data.get("value", []))
        return v

    async def patch_observation(
        self, observation_id: str, obs_patch: Dict[str, Any]
    ) -> httpx.Response:
        merged = {
            **(await self._auth_headers()),
            "commit-message": "QC update",
        }
        return await self._request(
            "PATCH",
            f"/Observations({observation_id})",
            json=obs_patch,
            headers=merged,
        )
