import argparse
import logging

from .client import build_istsos_client
from .config import (
    load_config,
    observation_mode,
    overwrite_observations_enabled,
)
from .importers.common import import_result_details
from .importers.ufam import process_ufam
from .importers.vulink_varese import process_vulink_varese
from .logging_config import configure_logging, source_log_context
from .remote import list_remote, remote_dir


def build_parser():
    parser = argparse.ArgumentParser(
        description="Import FTP observations into the configured istSOS API."
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to the YAML configuration file.",
    )
    return parser


def print_listing(index, item, entries):
    protocol = (item.get("protocol") or "ftp").lower()
    name = item.get("type") or f"ftp-{index}"
    host = item.get("host", "unknown-host")
    directory = remote_dir(item)

    print(f"\n[{index}] {name} - {protocol}://{host}{directory}")
    if not entries:
        print("  <empty>")
        return

    for entry in entries:
        print(f"  {entry}")


def print_import_result(index, item, result):
    protocol = (item.get("protocol") or "ftp").lower()
    name = item.get("type") or f"ftp-{index}"
    host = item.get("host", "unknown-host")
    directory = remote_dir(item)
    print(f"\n[{index}] {name} - {protocol}://{host}{directory}")
    for detail in import_result_details(result).split(", "):
        print(f"  {detail}")


def source_label(index, item):
    if not isinstance(item, dict):
        return f"[{index}] invalid item"
    name = item.get("type") or f"ftp-{index}"
    host = item.get("host", "unknown-host")
    directory = remote_dir(item)
    return f"[{index}] {name} - {host}{directory}"


def print_run_summary(summary):
    print("\nSummary")
    if not summary:
        print("  No configured sources were processed.")
        return

    for item in summary:
        print(f"  {item['label']}")
        print(f"    status: {item['status']}")
        if item.get("details"):
            print(f"    {item['details']}")
        if item.get("log_file"):
            print(f"    log: {item['log_file']}")
        if item.get("error"):
            print(f"    error: {item['error']}")


def build_ready_istsos_client(config):
    istSOS = build_istsos_client(config)
    istSOS.ensure_token()
    istSOS.fetch_datastreams()
    return istSOS


def set_source_observation_policy(config, item, client):
    client.update = overwrite_observations_enabled(config, item)
    client.observation_mode = observation_mode(config, item)


def main():
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    configure_logging(config)
    istSOS = None

    failures = 0
    summary = []
    for index, item in enumerate(config["ftps"], start=1):
        if not isinstance(item, dict):
            print(f"\n[{index}] invalid item: expected a mapping")
            failures += 1
            summary.append(
                {
                    "label": source_label(index, item),
                    "status": "failed",
                    "error": "expected a mapping",
                }
            )
            continue

        label = source_label(index, item)
        with source_log_context(config, index, label) as source_log_file:
            log_file = str(source_log_file)
            try:
                result = None
                if item.get("type") == "ufam":
                    if istSOS is None:
                        istSOS = build_ready_istsos_client(config)
                    set_source_observation_policy(config, item, istSOS)
                    result = process_ufam(item, istSOS)
                elif item.get("type") == "vulink-varese":
                    if istSOS is None:
                        istSOS = build_ready_istsos_client(config)
                    set_source_observation_policy(config, item, istSOS)
                    result = process_vulink_varese(item, istSOS)

                if result is not None:
                    print_import_result(index, item, result)
                    status = "succeeded"
                    if result["error"]:
                        status = "completed with errors"
                    summary.append(
                        {
                            "label": label,
                            "status": status,
                            "details": import_result_details(result),
                            "log_file": log_file,
                        }
                    )
                    continue

                entries = list_remote(item)
            except Exception as exc:
                name = item.get("type") or f"ftp-{index}"
                print(f"\n[{index}] {name} - ERROR: {exc}")
                logging.exception("Source %s failed", label)
                failures += 1
                summary.append(
                    {
                        "label": label,
                        "status": "failed",
                        "error": str(exc),
                        "log_file": log_file,
                    }
                )
                continue

            print_listing(index, item, entries)
            summary.append(
                {
                    "label": label,
                    "status": "succeeded",
                    "details": f"listed entries: {len(entries)}",
                    "log_file": log_file,
                }
            )

    print_run_summary(summary)
    if failures:
        raise SystemExit(1)
