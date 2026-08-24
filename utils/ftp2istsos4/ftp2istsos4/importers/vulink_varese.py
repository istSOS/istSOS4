import logging
import posixpath
import zipfile

from ..observations import (
    post_observations,
    sensor_things_observations,
)
from ..remote import (
    archive_ftp_item,
    connect_ftp,
    download_ftp_file,
    ensure_ftp_dir,
    ftp_name,
)
from .common import import_result, source_log


def read_text_from_zip(zip_file, member):
    raw = zip_file.read(member)
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def filename_suffixes(item):
    files = item.get("files") or []
    suffixes = []
    for file_config in files:
        if not isinstance(file_config, dict):
            continue
        suffix = file_config.get("filename_suffix")
        if suffix:
            suffixes.append(suffix)
    return suffixes


def files_by_suffix(item):
    files = item.get("files") or []
    result = {}
    for file_config in files:
        if not isinstance(file_config, dict):
            continue
        suffix = file_config.get("filename_suffix")
        columns = file_config.get("columns") or []
        if suffix and columns:
            result[suffix] = file_config
    return result


def member_matches_suffix(member, suffixes):
    name = posixpath.basename(member)
    return any(name.endswith(suffix) for suffix in suffixes)


def file_config_for_member(member, files_config):
    name = posixpath.basename(member)
    for suffix, file_config in files_config.items():
        if name.endswith(suffix):
            return file_config
    return None


def post_vulink_varese_zip(client, zip_buffer, files_config, suffixes, tz_name):
    posted = 0
    updated = 0
    skipped_duplicates = 0
    with zipfile.ZipFile(zip_buffer, "r") as zip_file:
        all_members = [
            member for member in zip_file.namelist() if not member.endswith("/")
        ]
        members = [
            member
            for member in all_members
            if member_matches_suffix(member, suffixes)
        ]
        if not members:
            raise ValueError(
                "No matching files in zip. "
                f"Configured suffixes: {suffixes}. "
                f"Zip members: {all_members}"
            )

        for member in members:
            file_config = file_config_for_member(member, files_config)
            if file_config is None:
                continue

            print(f"  parse {member}", flush=True)
            text = read_text_from_zip(zip_file, member)
            observations, skipped_existing = sensor_things_observations(
                client, text, file_config, tz_name
            )
            print(f"  observations parsed: {len(observations)}", flush=True)
            if observations:
                member_posted, member_skipped, member_updated = (
                    post_observations(client, observations, member)
                )
            else:
                member_posted = 0
                member_skipped = 0
                member_updated = 0
            posted += member_posted
            updated += member_updated
            skipped_duplicates += member_skipped + skipped_existing

    if posted == 0 and updated == 0 and skipped_duplicates == 0:
        raise ValueError("Zip produced 0 posted or updated observations")
    return posted, skipped_duplicates, updated


def process_vulink_varese(item, client):
    files_config = files_by_suffix(item)
    suffixes = list(files_config)
    if not suffixes:
        raise ValueError(
            "vulink-varese: missing files with configured columns"
        )
    tz_name = item.get("tz") or "UTC"
    sent_dir = item.get("sent_dir") or "sent"
    error_dir = item.get("error_dir") or "error"
    result = import_result()

    with connect_ftp(item) as ftp:
        ensure_ftp_dir(ftp, sent_dir)
        ensure_ftp_dir(ftp, error_dir)
        source_log(item, "FTP list files")
        entries = ftp.nlst()

    zip_names = [
        ftp_name(entry)
        for entry in entries
        if ftp_name(entry).lower().endswith(".zip")
    ]
    source_log(item, f"FTP zip files found: {len(zip_names)}")

    if not zip_names:
        return result

    for zip_index, zip_name in enumerate(zip_names, start=1):
        result["processed"] += 1
        source_log(
            item, f"FTP download {zip_index}/{len(zip_names)}: {zip_name}"
        )

        try:
            zip_buffer = download_ftp_file(item, zip_name)
            posted, skipped_duplicates, updated = post_vulink_varese_zip(
                client, zip_buffer, files_config, suffixes, tz_name
            )
        except Exception as exc:
            result["error"] += 1
            source_log(item, f"ERROR processing {zip_name}: {exc}")
            logging.exception("Processing failed for %s", zip_name)
            if client.dry_run:
                source_log(
                    item, f"DRY RUN: not moving {zip_name} to {error_dir}"
                )
                continue
            try:
                target = archive_ftp_item(item, zip_name, error_dir)
                source_log(item, f"FTP moved to {target}")
            except Exception as move_exc:
                source_log(
                    item, f"ERROR moving {zip_name} to {error_dir}: {move_exc}"
                )
                logging.exception(
                    "Moving %s to %s failed", zip_name, error_dir
                )
            continue

        result["posted"] += posted
        result["updated"] += updated
        result["skipped_duplicates"] += skipped_duplicates
        if client.dry_run:
            source_log(
                item,
                f"DRY RUN: not moving {zip_name} "
                f"after printing {posted} observations",
            )
            continue

        try:
            target = archive_ftp_item(item, zip_name, sent_dir)
        except Exception as move_exc:
            result["error"] += 1
            source_log(
                item,
                f"ERROR moving successfully posted {zip_name} to "
                f"{sent_dir}: {move_exc}",
            )
            logging.exception("Moving %s to %s failed", zip_name, sent_dir)
            continue

        result["sent"] += 1
        source_log(
            item,
            f"FTP moved to {target} after posting {posted} "
            f"observations and skipping {skipped_duplicates} duplicates",
        )

    return result
