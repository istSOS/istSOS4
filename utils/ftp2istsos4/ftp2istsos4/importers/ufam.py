import logging

from ..observations import post_observations, sensor_things_observations
from ..remote import read_remote_text_file, remote_file_path
from .common import import_result, source_log


def configured_file_path(file_config):
    return file_config.get("file_path") or file_config.get("path")


def configured_ufam_files(item):
    files = item.get("files") or []
    if not isinstance(files, list):
        raise ValueError("ufam: files must be a list")
    if not files:
        raise ValueError("ufam: missing files list")
    return files


def print_remote_file(path, text):
    print(f"\n--- {path} ---")
    print(text, end="" if text.endswith("\n") else "\n")


def process_ufam(item, client):
    result = import_result(printed=0)
    tz_name = item.get("tz") or "UTC"

    for file_index, file_config in enumerate(
        configured_ufam_files(item), start=1
    ):
        if not isinstance(file_config, dict):
            result["error"] += 1
            source_log(item, f"ERROR file {file_index}: expected a mapping")
            continue

        file_path = configured_file_path(file_config)
        if not file_path:
            result["error"] += 1
            source_log(
                item,
                f"ERROR file {file_index}: missing required 'file_path'",
            )
            continue

        full_path = remote_file_path(item, file_path)
        try:
            source_log(item, f"read remote file {full_path}")
            text = read_remote_text_file(item, file_path)
            if client.dry_run:
                print_remote_file(full_path, text)
                result["printed"] += 1

            source_log(item, f"parse {full_path}")
            observations = sensor_things_observations(
                client, text, file_config, tz_name
            )
            source_log(
                item,
                f"observations parsed from {full_path}: "
                f"{len(observations)}",
            )
            if not observations:
                raise ValueError(f"{full_path} produced 0 observations")

            posted, skipped_duplicates = post_observations(
                client, observations, full_path
            )
        except Exception as exc:
            result["error"] += 1
            source_log(item, f"ERROR processing {full_path}: {exc}")
            logging.exception("Processing failed for %s", full_path)
            continue

        result["processed"] += 1
        result["posted"] += posted
        result["skipped_duplicates"] += skipped_duplicates

    return result
