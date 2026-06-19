from __future__ import annotations

import logging
import math
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger("mqtt2istsos")
CONFIG_PATH_ENV = "MQTT2ISTSOS_CONFIG"
NULL_VALUES = {"", "null", "none", "nan", "na", "n/a"}
NULL_RESULT_VALUE = -999
NULL_RESULT_QUALITY = "00"
DEFAULT_RESULT_QUALITY = "11"
SKIP_DATASTREAM_NAMES = {"skip"}
SUCCESS_LEVEL = 35
NOTICE_LEVEL = 60
RESET = "\033[0m"
WHITE = "\033[37m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"


class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: WHITE,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: RED,
        SUCCESS_LEVEL: GREEN,
        NOTICE_LEVEL: WHITE,
    }

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = self.COLORS.get(record.levelno, WHITE)
        return f"{color}{message}{RESET}"


def success(
    self: logging.Logger, message: str, *args: Any, **kwargs: Any
) -> None:
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


def notice(
    self: logging.Logger, message: str, *args: Any, **kwargs: Any
) -> None:
    if self.isEnabledFor(NOTICE_LEVEL):
        self._log(NOTICE_LEVEL, message, args, **kwargs)


logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")
logging.addLevelName(NOTICE_LEVEL, "NOTICE")
logging.Logger.success = success  # type: ignore[attr-defined]
logging.Logger.notice = notice  # type: ignore[attr-defined]


@dataclass(frozen=True)
class Config:
    mqtt_host: str
    mqtt_port: int
    mqtt_client_id: str
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_keepalive: int
    mqtt_qos: int
    mqtt_tls: bool
    mqtt_tls_insecure: bool
    mqtt_topics: list[str]
    payload_separator: str
    reconnect_delay_sec: float
    queue_maxsize: int

    istsos_url: str
    istsos_username: str
    istsos_password: str
    istsos_timeout_sec: int
    commit_message: str | None

    mapping: dict[str, list[str]]
    dry_run: bool


@dataclass(frozen=True)
class MqttMessage:
    topic: str
    payload: bytes


def configure_logging(level_name: str = "INFO") -> None:
    level_name = level_name.upper()
    level = getattr(logging, level_name, logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColorFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)


def read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Configuration file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError("Configuration file must contain a YAML object")
    return data


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list):
        return [str(part).strip() for part in value if str(part).strip()]
    raise RuntimeError("Expected a list")


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_payload_separator(value: Any) -> str:
    if value is None:
        return ","
    separator = str(value)
    if separator == "":
        raise RuntimeError("Set mqtt.payload_separator to a non-empty string")
    return separator


def required_text(
    values: dict[str, Any], key: str, section: str, dry_run: bool = False
) -> str:
    value = clean_text(values.get(key))
    if value or dry_run:
        return value or ""
    raise RuntimeError(f"Set {section}.{key} in config.yaml")


def load_mapping(data: Any) -> dict[str, list[str]]:
    if not isinstance(data, dict):
        raise RuntimeError("Set mapping in config.yaml")

    mapping: dict[str, list[str]] = {}
    for topic, datastreams in data.items():
        if not isinstance(datastreams, list):
            raise RuntimeError(f"Mapping for topic {topic!r} must be a list")

        clean_names = [
            str(name).strip() for name in datastreams if str(name).strip()
        ]
        if clean_names:
            mapping[str(topic).strip()] = clean_names

    if not mapping:
        raise RuntimeError("mapping does not contain any datastreams")
    return mapping


def load_config() -> Config:
    config_path = Path(os.getenv(CONFIG_PATH_ENV, "config.yaml"))
    data = read_config_file(config_path)
    configure_logging(str(data.get("log_level", "INFO")))

    mqtt = data.get("mqtt") or {}
    istsos = data.get("istsos") or {}
    if not isinstance(mqtt, dict) or not isinstance(istsos, dict):
        raise RuntimeError("mqtt and istsos must be YAML objects")

    dry_run = as_bool(data.get("dry_run"), False)
    mapping = load_mapping(data.get("mapping"))
    topics = as_list(mqtt.get("topics")) or list(mapping.keys())
    config = Config(
        mqtt_host=clean_text(mqtt.get("host")) or "localhost",
        mqtt_port=int(mqtt.get("port", 1883)),
        mqtt_client_id=clean_text(mqtt.get("client_id")) or "mqtt2istsos",
        mqtt_username=clean_text(mqtt.get("username")),
        mqtt_password=clean_text(mqtt.get("password")),
        mqtt_keepalive=int(mqtt.get("keepalive", 60)),
        mqtt_qos=int(mqtt.get("qos", 0)),
        mqtt_tls=as_bool(mqtt.get("tls"), False),
        mqtt_tls_insecure=as_bool(mqtt.get("tls_insecure"), False),
        mqtt_topics=topics,
        payload_separator=parse_payload_separator(
            mqtt.get("payload_separator")
        ),
        reconnect_delay_sec=float(mqtt.get("reconnect_delay_sec", 10.0)),
        queue_maxsize=int(mqtt.get("queue_maxsize", 1000)),
        istsos_url=required_text(istsos, "url", "istsos", dry_run),
        istsos_username=required_text(istsos, "username", "istsos", dry_run),
        istsos_password=required_text(istsos, "password", "istsos", dry_run),
        istsos_timeout_sec=int(istsos.get("timeout_sec", 15)),
        commit_message=clean_text(istsos.get("commit_message")),
        mapping=mapping,
        dry_run=dry_run,
    )
    LOGGER.notice(
        "Loaded configuration: path=%s, mqtt=%s:%s, topics=%d, mappings=%d, dry_run=%s, istsos_url=%s",
        config_path,
        config.mqtt_host,
        config.mqtt_port,
        len(config.mqtt_topics),
        len(config.mapping),
        config.dry_run,
        config.istsos_url,
    )

    return config


def topic_matches(pattern: str, topic: str) -> bool:
    if pattern == topic:
        return True

    pattern_parts = pattern.split("/")
    topic_parts = topic.split("/")

    for index, pattern_part in enumerate(pattern_parts):
        if pattern_part == "#":
            return index == len(pattern_parts) - 1
        if index >= len(topic_parts):
            return False
        if pattern_part != "+" and pattern_part != topic_parts[index]:
            return False

    return len(pattern_parts) == len(topic_parts)


def datastreams_for_topic(
    topic: str, mapping: dict[str, list[str]]
) -> list[str] | None:
    if topic in mapping:
        return mapping[topic]

    for pattern, datastreams in mapping.items():
        if topic_matches(pattern, topic):
            return datastreams
    return None


def parse_payload(
    payload: bytes, separator: str = ","
) -> tuple[list[str], str]:
    parts = [
        part.strip() for part in payload.decode("utf-8").split(separator)
    ]
    if len(parts) < 2:
        raise ValueError(
            f"Expected payload: timestamp{separator}value1{separator}value2..."
        )

    timestamp, *values = parts
    datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return values, timestamp


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def is_null_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip().lower() in NULL_VALUES


def is_skip_datastream(name: str) -> bool:
    return name.strip().lower() in SKIP_DATASTREAM_NAMES


def normalize_result(value: Any) -> Any | None:
    if is_null_value(value):
        return None

    text = str(value).strip()
    numeric_text = (
        text.replace(",", ".") if "," in text and "." not in text else text
    )
    for cast in (int, float):
        try:
            return cast(numeric_text)
        except ValueError:
            continue
    return None


def build_observation(
    datastream: dict[str, Any],
    phenomenon_time: str,
    result_time: str,
    result: Any,
    result_quality: str = DEFAULT_RESULT_QUALITY,
) -> dict[str, Any]:
    return {
        "Datastream": datastream,
        "phenomenonTime": phenomenon_time,
        "resultTime": result_time,
        "result": result,
        "resultQuality": result_quality,
    }


class Processor:
    def __init__(self, config: Config, istsos_client: Any | None):
        self.config = config
        self.istsos_client = istsos_client
        self.datastream_ids: dict[str, int] = {}

    def process(self, message: MqttMessage) -> None:
        datastreams = datastreams_for_topic(message.topic, self.config.mapping)
        if not datastreams:
            LOGGER.warning(
                "No datastream mapping for MQTT topic %s", message.topic
            )
            return

        try:
            values, phenomenon_time = parse_payload(
                message.payload, self.config.payload_separator
            )
        except Exception:
            LOGGER.exception(
                "Could not parse MQTT payload on %s", message.topic
            )
            return

        result_time = now_utc_iso()
        inserted = 0
        skipped = 0

        if len(values) > len(datastreams):
            LOGGER.debug(
                "Ignoring %d extra values on %s",
                len(values) - len(datastreams),
                message.topic,
            )
        if len(values) < len(datastreams):
            LOGGER.warning(
                "Topic %s has %d values for %d datastreams",
                message.topic,
                len(values),
                len(datastreams),
            )

        for datastream_name, raw_value in zip(datastreams, values):
            if is_skip_datastream(datastream_name):
                skipped += 1
                LOGGER.debug(
                    "Skipping mapped value on %s because datastream name is %r",
                    message.topic,
                    datastream_name,
                )
                continue

            if is_null_value(raw_value):
                result = NULL_RESULT_VALUE
                result_quality = NULL_RESULT_QUALITY
            else:
                result = normalize_result(raw_value)
                result_quality = DEFAULT_RESULT_QUALITY
                if result is None:
                    LOGGER.warning(
                        "Skipping %s: invalid numeric value %r",
                        datastream_name,
                        raw_value,
                    )
                    skipped += 1
                    continue

            if self.config.dry_run:
                observation = build_observation(
                    {"name": datastream_name},
                    phenomenon_time,
                    result_time,
                    result,
                    result_quality,
                )
                LOGGER.info("DRY_RUN %s -> %s", datastream_name, observation)
                inserted += 1
                continue

            datastream_id = self.datastream_id(datastream_name)
            if datastream_id is None:
                LOGGER.warning(
                    "Skipping %s: datastream was not found", datastream_name
                )
                skipped += 1
                continue

            observation = build_observation(
                {"@iot.id": datastream_id},
                phenomenon_time,
                result_time,
                result,
                result_quality,
            )

            try:
                self.istsos_client.insert_observation(
                    observation,
                    datastream_name,
                    commit_message=self.config.commit_message,
                )
                inserted += 1
                LOGGER.success(
                    "Inserted %s at %s: result=%s",
                    datastream_name,
                    phenomenon_time,
                    result,
                )
            except Exception:
                skipped += 1
                LOGGER.exception(
                    "Could not insert observation for %s", datastream_name
                )

        LOGGER.info(
            "Processed %s at %s: inserted=%d skipped=%d",
            message.topic,
            phenomenon_time,
            inserted,
            skipped,
        )

    def datastream_id(self, name: str) -> int | None:
        cached = self.datastream_ids.get(name)
        if cached is not None:
            return cached

        try:
            datastream_id = self.istsos_client.get_datastream_id(name)
        except Exception:
            LOGGER.exception("Could not resolve datastream %s", name)
            return None

        if datastream_id is not None:
            self.datastream_ids[name] = datastream_id
        return datastream_id


def worker(
    messages: queue.Queue[MqttMessage | None], processor: Processor
) -> None:
    while True:
        message = messages.get()
        try:
            if message is None:
                return
            processor.process(message)
        finally:
            messages.task_done()


def build_mqtt_client(
    config: Config,
    messages: queue.Queue[MqttMessage | None],
    stop_event: threading.Event,
) -> Any:
    from paho.mqtt import client as mqtt

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=config.mqtt_client_id,
    )
    client.reconnect_delay_set(
        min_delay=max(1, int(config.reconnect_delay_sec)),
        max_delay=max(1, int(config.reconnect_delay_sec)),
    )

    if config.mqtt_username:
        client.username_pw_set(config.mqtt_username, config.mqtt_password)
    if config.mqtt_tls:
        client.tls_set()
        client.tls_insecure_set(config.mqtt_tls_insecure)

    def on_connect(
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if getattr(reason_code, "is_failure", False):
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return

        LOGGER.info(
            "Connected to MQTT broker %s:%d",
            config.mqtt_host,
            config.mqtt_port,
        )
        for topic in config.mqtt_topics:
            result, mid = client.subscribe(topic, qos=config.mqtt_qos)
            LOGGER.info(
                "Subscribe requested: topic=%s result=%s mid=%s",
                topic,
                result,
                mid,
            )

    def on_subscribe(
        client: Any,
        userdata: Any,
        mid: int,
        reason_codes: Any,
        properties: Any,
    ) -> None:
        LOGGER.info(
            "Subscribe acknowledged: mid=%s reason_codes=%s", mid, reason_codes
        )

    def on_disconnect(
        client: Any,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if not stop_event.is_set():
            LOGGER.warning(
                "MQTT disconnected (%s); retrying in %.1fs",
                reason_code,
                config.reconnect_delay_sec,
            )

    def on_message(client: Any, userdata: Any, mqtt_message: Any) -> None:
        payload_text = mqtt_message.payload.decode("utf-8", errors="replace")
        LOGGER.info(
            "Received MQTT message: topic=%s payload=%s",
            mqtt_message.topic,
            payload_text[:300],
        )
        message = MqttMessage(
            topic=mqtt_message.topic,
            payload=bytes(mqtt_message.payload),
        )
        try:
            messages.put_nowait(message)
        except queue.Full:
            LOGGER.error(
                "Queue full; dropping MQTT message on %s", mqtt_message.topic
            )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    return client


def run(config: Config) -> None:
    istsos_client = None
    if not config.dry_run:
        from utils.istsosClient import IstsosClient

        istsos_client = IstsosClient(
            config.istsos_url,
            config.istsos_username,
            config.istsos_password,
            timeout_sec=config.istsos_timeout_sec,
        )

    messages: queue.Queue[MqttMessage | None] = queue.Queue(
        maxsize=config.queue_maxsize
    )
    stop_event = threading.Event()
    processor = Processor(config, istsos_client)
    thread = threading.Thread(
        target=worker, args=(messages, processor), daemon=True
    )
    thread.start()

    mqtt_client = build_mqtt_client(config, messages, stop_event)

    def stop(signum: int, frame: Any) -> None:
        LOGGER.info("Stopping on signal %s", signum)
        stop_event.set()
        mqtt_client.disconnect()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while not stop_event.is_set():
            try:
                LOGGER.info(
                    "Connecting to MQTT broker %s:%d",
                    config.mqtt_host,
                    config.mqtt_port,
                )
                mqtt_client.connect(
                    config.mqtt_host,
                    config.mqtt_port,
                    keepalive=config.mqtt_keepalive,
                )
                mqtt_client.loop_forever(retry_first_connection=True)
            except KeyboardInterrupt:
                stop_event.set()
            except Exception:
                if not stop_event.is_set():
                    LOGGER.exception(
                        "MQTT loop failed; retrying in %.1fs",
                        config.reconnect_delay_sec,
                    )
                    time.sleep(config.reconnect_delay_sec)
    finally:
        stop_event.set()
        mqtt_client.disconnect()
        messages.join()
        messages.put(None)
        messages.join()
        thread.join(timeout=5)
        if istsos_client is not None:
            istsos_client.close()


def main() -> None:
    try:
        config = load_config()
        LOGGER.notice(
            "Configured %d MQTT topics and %d mappings",
            len(config.mqtt_topics),
            len(config.mapping),
        )
        run(config)
    except Exception:
        LOGGER.exception("Fatal error")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
