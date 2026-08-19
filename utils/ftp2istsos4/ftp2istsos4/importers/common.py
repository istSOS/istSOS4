from ..remote import remote_dir


def import_result(**extra):
    result = {
        "processed": 0,
        "sent": 0,
        "error": 0,
        "posted": 0,
        "updated": 0,
        "skipped_duplicates": 0,
    }
    result.update(extra)
    return result


def source_context(item):
    source_type = item.get("type") or "ftp"
    host = item.get("host", "unknown-host")
    directory = remote_dir(item)
    return f"{source_type} {host}{directory}"


def source_log(item, message):
    print(f"[{source_context(item)}] {message}", flush=True)


def import_result_details(result):
    labels = [
        ("processed", "files processed"),
        ("sent", "moved to sent"),
        ("error", "errors"),
        ("posted", "posted observations"),
        ("updated", "updated observations"),
        ("skipped_duplicates", "skipped existing/duplicates"),
        ("skipped_invalid_rows", "skipped invalid rows"),
        ("printed", "printed files"),
    ]
    details = []
    for key, label in labels:
        if key in result:
            details.append(f"{label}: {result[key]}")
    return ", ".join(details)
