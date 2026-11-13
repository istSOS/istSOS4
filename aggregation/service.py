"""Scheduled pandas aggregation with persistent run state; scheduling uses UTC."""

import argparse
import json
import logging
import math
import os
import signal
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Event
from urllib.parse import unquote, urlencode, urljoin, urlsplit, urlunsplit

import requests
from croniter import croniter
from istsos4_client import Client
from istsos4_client.models import Datastream

from aggregation.calculation import CalculationError, timestamp
from aggregation.engine import calculate
from aggregation.state import ProcessingState, StateError

logger = logging.getLogger("aggregation")
INTERVALS = ("PT10M", "PT1H", "P1D", "P7D", "P1M", "P1Y")


class ApiError(CalculationError):
    """A remote failure safe to print without credentials or response bodies."""


def boolean(value, name):
    if value.lower() in {"true", "1"}:
        return True
    if value.lower() in {"false", "0"}:
        return False
    raise ValueError(f"{name} must be true/false or 1/0")


def next_execution(schedule, after):
    if len(schedule.split()) != 5:
        raise ValueError("AGGREGATION_SCHEDULE must have five cron fields")
    return croniter(schedule, after.astimezone(UTC)).get_next(datetime)


@dataclass(frozen=True)
class Settings:
    api_url: str
    schedule: str = "*/10 * * * *"
    utc_offset_minutes: int = 60
    authorization: bool = False
    username: str = field(default="", repr=False)
    password: str = field(default="", repr=False)
    timeout_seconds: float = 30
    run_on_startup: bool = False
    write_batch_size: int = 1000

    @classmethod
    def from_env(cls):
        settings = cls(
            api_url=os.getenv(
                "AGGREGATION_API_URL", "http://api:5000/istsos4/v1.1"
            ).rstrip("/"),
            schedule=os.getenv("AGGREGATION_SCHEDULE", "*/10 * * * *"),
            write_batch_size=int(os.getenv("AGGREGATION_WRITE_BATCH_SIZE", "1000")),
            utc_offset_minutes=int(os.getenv("AGGREGATION_UTC_OFFSET_MINUTES", "60")),
            authorization=boolean(os.getenv("AUTHORIZATION", "0"), "AUTHORIZATION"),
            username=os.getenv("ISTSOS_ADMIN", ""),
            password=os.getenv("ISTSOS_ADMIN_PASSWORD", ""),
            timeout_seconds=float(
                os.getenv("AGGREGATION_REQUEST_TIMEOUT_SECONDS", "30")
            ),
            run_on_startup=boolean(
                os.getenv("AGGREGATION_RUN_ON_STARTUP", "false"),
                "AGGREGATION_RUN_ON_STARTUP",
            ),
        )
        url = urlsplit(settings.api_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "AGGREGATION_API_URL must be an HTTP(S) API root without credentials, query or fragment"
            )
        if not -840 <= settings.utc_offset_minutes <= 840:
            raise ValueError(
                "AGGREGATION_UTC_OFFSET_MINUTES must be between -840 and 840"
            )
        if not math.isfinite(settings.timeout_seconds) or settings.timeout_seconds <= 0:
            raise ValueError(
                "AGGREGATION_REQUEST_TIMEOUT_SECONDS must be finite and positive"
            )
        if settings.authorization and (not settings.username or not settings.password):
            raise ValueError(
                "ISTSOS_ADMIN and ISTSOS_ADMIN_PASSWORD are required when AUTHORIZATION is enabled"
            )
        if not 1 <= settings.write_batch_size <= 10000:
            raise ValueError("AGGREGATION_WRITE_BATCH_SIZE must be between 1 and 10000")
        next_execution(settings.schedule, datetime.now(UTC))
        return settings


class SensorThings:
    """Use the project client, with a small adapter for the custom config API.

    istsos4-client 0.4.0 has no AggregationConfigurations model/raw GET API.
    Only get_document() uses its private transport/auth helpers; keep that
    dependency isolated here until the client supports this extension.
    """

    def __init__(self, settings, stop):
        self.settings = settings
        self.stop = stop
        self.client = None

    def begin_cycle(self):
        self.close()
        self.client = Client(
            self.settings.api_url,
            username=self.settings.username if self.settings.authorization else None,
            password=self.settings.password if self.settings.authorization else None,
            timeout=self.settings.timeout_seconds,
            max_retries=2,
            retry_delay=1,
        )

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

    def url(self, link, current=None):
        # Public nextLinks may point to localhost/the reverse proxy. Retain only
        # the API path/query and always send credentials to the configured host.
        target = urlsplit(urljoin(current or self.settings.api_url + "/", link))
        base = urlsplit(self.settings.api_url)
        path = unquote(target.path)
        if not path.startswith(base.path + "/") or any(
            part in {".", ".."} for part in path.split("/")
        ):
            raise ApiError("Pagination link is outside the configured API root")
        return urlunsplit((base.scheme, base.netloc, target.path, target.query, ""))

    @staticmethod
    def failure(error):
        if isinstance(error, requests.HTTPError) and error.response is not None:
            return ApiError(f"API returned HTTP {error.response.status_code}")
        return ApiError("API request failed or returned an invalid response")

    def get_document(self, url):
        if self.stop.is_set():
            raise ApiError("Service is stopping")
        try:
            response = self.client._request(
                "GET",
                url,
                headers=self.client._headers(),
                allow_redirects=False,
            )
            if response.status_code != 200:
                raise ApiError(f"API returned HTTP {response.status_code}")
            document = response.json()
        except (requests.RequestException, ValueError, KeyError) as error:
            raise self.failure(error) from None
        if not isinstance(document, dict):
            raise ApiError("API response must be a JSON object")
        return document

    def datastream(self, identifier):
        try:
            datastream = self.client.get(Datastream, identifier)
        except (requests.RequestException, ValueError, KeyError) as error:
            raise self.failure(error) from None
        if datastream.iot_id != identifier:
            raise ApiError("Datastream response has an unexpected @iot.id")
        return datastream.serialize()

    def configurations(self):
        url = self.url("AggregationConfigurations?enabled=true&$top=100")
        seen = set()
        configurations = []
        while url:
            if url in seen:
                raise ApiError("API returned a repeated pagination link")
            seen.add(url)
            document = self.get_document(url)
            values = document.get("value")
            if not isinstance(values, list) or not all(
                isinstance(item, dict) for item in values
            ):
                raise ApiError(
                    "Configuration collection must contain a value array of objects"
                )
            configurations.extend(
                item for item in values if item.get("enabled") is True
            )
            link = document.get("@iot.nextLink")
            if link is not None and (not isinstance(link, str) or not link):
                raise ApiError("API returned an invalid pagination link")
            url = self.url(link, current=url) if link else None
        return configurations

    def observation_pages(self, identifier, options):
        options = {"$top": 2000, "$count": "false", **options}
        url = self.url(f"Datastreams({identifier})/Observations?" + urlencode(options))
        seen = set()
        while url:
            if url in seen:
                raise ApiError("Repeated observation pagination link")
            seen.add(url)
            document = self.get_document(url)
            values = document.get("value")
            if not isinstance(values, list) or not all(
                isinstance(item, dict) for item in values
            ):
                raise ApiError(
                    "Observation collection must contain an array of objects"
                )
            yield values
            link = document.get("@iot.nextLink")
            if link is not None and (not isinstance(link, str) or not link):
                raise ApiError("Invalid observation pagination link")
            url = self.url(link, current=url) if link else None

    def first_observation(self, identifier, cutoff, versioned, as_of=None):
        options = {
            "$top": 1,
            "$orderby": "phenomenonTime asc,id asc",
            "$filter": f"phenomenonTime le {cutoff.isoformat()}",
            "$select": "id,phenomenonTime",
        }
        if versioned:
            options["$as_of"] = (as_of or cutoff).isoformat()
        pages = self.observation_pages(identifier, options)
        try:
            for page in pages:
                if page:
                    return page[0]["phenomenonTime"]
        finally:
            pages.close()
        return None

    def observations(self, identifier, start, end, lower, upper, as_of=None):
        options = {
            "$orderby": "phenomenonTime asc,id asc",
            "$filter": f"phenomenonTime {lower} {start.isoformat()} and phenomenonTime {upper} {end.isoformat()}",
            "$select": "id,phenomenonTime,result,resultQuality",
        }
        if as_of is not None:
            options["$as_of"] = as_of.isoformat()
        return self.observation_pages(identifier, options)

    def changed_observations(self, identifier, since, until):
        return self.observation_pages(
            identifier,
            {
                "$from_to": since.isoformat() + "/" + until.isoformat(),
                "$select": "id,phenomenonTime,systemTimeValidity",
            },
        )


def describe(configuration, datastream, settings):
    warnings = []
    properties = datastream.get("properties")
    if properties is None:
        properties = {}
    if not isinstance(properties, dict):
        properties = {}
        warnings.append("Datastream properties must be an object")
    sampling = properties.get("samplingFrequency")
    if configuration.get("mode") == "sampled" and (
        not isinstance(sampling, str) or not sampling.strip()
    ):
        warnings.append("Sampled Datastream has no samplingFrequency")
    constraints = properties.get("constraints", [])
    if not isinstance(constraints, list) or not all(
        isinstance(item, dict) for item in constraints
    ):
        constraint_count = None
        warnings.append("Datastream constraints must be an array of objects")
    else:
        constraint_count = len(constraints)
        if constraint_count > 28:
            warnings.append("QC layout exceeds the result bitmask capacity")
    offset = configuration.get("utcOffsetMinutes")
    if offset is None:
        offset = settings.utc_offset_minutes
    return {
        "event": "datastream_metadata",
        "readAt": datetime.now(UTC).isoformat(),
        "datastreamId": configuration["datastreamId"],
        "name": datastream.get("name"),
        "configuration": configuration,
        "qualityFiltering": "disabled"
        if configuration.get("ignoreQuality", False)
        else "enabled",
        "properties": properties,
        "samplingFrequency": sampling,
        "constraintCount": constraint_count,
        "effectiveUtcOffsetMinutes": offset,
        "intervals": INTERVALS,
        "warnings": warnings,
    }


def run_cycle(
    api, settings, stop, state=None, aggregate=False, manual=None, datastream_id=None
):
    logger.info("Reading enabled aggregation configurations")
    try:
        api.begin_cycle()
        configurations = api.configurations()
    except ApiError as error:
        logger.error("Discovery failed: %s", error)
        if state is not None:
            if aggregate:
                state.discovery_failed(str(error), phase="aggregation")
            else:
                state.discovery_failed(str(error))
        return False
    succeeded = failed = 0
    for configuration in configurations:
        if (
            datastream_id is not None
            and configuration.get("datastreamId") != datastream_id
        ):
            continue
        if stop.is_set():
            return False
        identifier = configuration.get("datastreamId")
        run_id = (
            (
                state.start(configuration["id"], phase="aggregation")
                if aggregate
                else state.start(configuration["id"])
            )
            if state is not None
            else None
        )
        if state is not None and run_id is None:
            continue  # Configuration was disabled/deleted since collection read.
        try:
            if type(identifier) is not int or identifier < 1:
                raise ApiError("Configuration contains an invalid datastreamId")
            datastream = api.datastream(identifier)
            result = describe(configuration, datastream, settings)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if aggregate:
                count = calculate(
                    api,
                    state,
                    configuration,
                    datastream,
                    settings,
                    run_id,
                    stop,
                    manual,
                )
                logger.info(
                    "Datastream %s: saved %s aggregate buckets", identifier, count
                )
                succeeded += 1
                continue
            status = "partial" if result["warnings"] else "success"
            if state is not None:
                error = (
                    {
                        "step": "metadata_validation",
                        "message": "; ".join(result["warnings"]),
                    }
                    if result["warnings"]
                    else None
                )
                state.finish(configuration["id"], run_id, status, error)
            succeeded += 1
        except (ApiError, CalculationError) as error:
            failed += 1
            logger.error("Cannot read Datastream %s: %s", identifier, error)
            if state is not None:
                state.finish(
                    configuration["id"],
                    run_id,
                    "interrupted" if stop.is_set() else "failed",
                    {
                        "step": "aggregation" if aggregate else "datastream_read",
                        "message": str(error),
                        **getattr(error, "context", {}),
                    },
                )
    logger.info(
        "%s finished: %s processed, %s failed",
        "Aggregation" if aggregate else "Discovery",
        succeeded,
        failed,
    )
    return failed == 0


def serve(settings, stop, task, now=lambda: datetime.now(UTC)):
    if settings.run_on_startup and not stop.is_set():
        task()
    due = next_execution(settings.schedule, now())
    while not stop.is_set():
        logger.info(
            "Next execution: %s (schedule=%s, timezone=UTC)",
            due.isoformat(),
            settings.schedule,
        )
        while not stop.is_set():
            remaining = (due - now()).total_seconds()
            if remaining <= 0:
                break
            stop.wait(min(remaining, 1))
        if stop.is_set():
            break
        task()  # Synchronous: there can be only one run in this process.
        finished = now()
        following = next_execution(settings.schedule, due)
        if following <= finished:
            logger.warning("Skipped overlapping/missed cron triggers while processing")
        due = next_execution(settings.schedule, max(finished, due))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one aggregation cycle and exit; requires enabled=true",
    )
    parser.add_argument(
        "--recompute-from",
        help="Manually recompute buckets intersecting this ISO datetime; requires --once",
    )
    parser.add_argument(
        "--recompute-to", help="Optional upper limit for manual recomputation"
    )
    parser.add_argument(
        "--datastream-id", type=int, help="Process only this configured Datastream"
    )
    args = parser.parse_args()
    if (args.recompute_from and not args.once) or (
        args.recompute_to and not args.recompute_from
    ):
        parser.error("Manual recomputation requires --once and --recompute-from")
    manual = None
    if args.recompute_from:
        try:
            manual = (
                timestamp(args.recompute_from),
                timestamp(args.recompute_to) if args.recompute_to else None,
            )
            if manual[1] is not None and manual[0] > manual[1]:
                raise CalculationError("Recomputation start must precede its end")
        except CalculationError as error:
            parser.error(str(error))
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    stop = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    try:
        enabled = boolean(
            os.getenv("AGGREGATION_ENABLED", "false"), "AGGREGATION_ENABLED"
        )
        if not enabled:
            logger.info("Aggregation disabled (AGGREGATION_ENABLED=false)")
            if not args.once:
                stop.wait()
            return 0
        settings = Settings.from_env()
    except (ValueError, OverflowError) as error:
        logger.error("Invalid aggregation settings: %s", error)
        return 2
    api = SensorThings(settings, stop)
    state = ProcessingState(
        host=os.getenv("POSTGRES_HOST", "database"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "istsos"),
        user=os.getenv("ISTSOS_ADMIN", "admin"),
        password=os.getenv("ISTSOS_ADMIN_PASSWORD", "admin"),
    )
    try:
        state.open()
        if args.once:
            return (
                0
                if run_cycle(
                    api,
                    settings,
                    stop,
                    state,
                    aggregate=True,
                    manual=manual,
                    datastream_id=args.datastream_id,
                )
                else 1
            )
        serve(
            settings,
            stop,
            lambda: run_cycle(
                api,
                settings,
                stop,
                state,
                aggregate=True,
                datastream_id=args.datastream_id,
            ),
        )
    except StateError as error:
        logger.error("%s", error)
        return 1
    finally:
        state.close()
        api.close()
    logger.info("Aggregation service stopped")
    return 0
