def import_yaml():
    try:
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "YAML support requires PyYAML. Install it with: "
            "python3 -m pip install -r requirements.txt"
        ) from exc
    return yaml


def import_paramiko():
    try:
        import paramiko
    except ImportError as exc:
        raise SystemExit(
            "SFTP support requires paramiko. Install it with: "
            "python3 -m pip install -r requirements.txt"
        ) from exc
    return paramiko


def import_requests():
    try:
        import requests
    except ImportError as exc:
        raise SystemExit(
            "HTTP support requires requests. Install it with: "
            "python3 -m pip install -r requirements.txt"
        ) from exc
    return requests
