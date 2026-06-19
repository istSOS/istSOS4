from pathlib import Path

from .dependencies import import_yaml


def load_config(config_path):
    yaml = import_yaml()
    path = Path(config_path)
    if not path.exists():
        raise SystemExit(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    if not isinstance(config, dict):
        raise SystemExit("Configuration root must be a mapping.")

    ftps = config.get("ftps")
    if not isinstance(ftps, list):
        raise SystemExit("Configuration must contain an 'ftps' list.")

    return config


def require_value(item, field):
    value = item.get(field)
    if value in (None, ""):
        name = item.get("type", "unnamed")
        raise ValueError(f"{name}: missing required field '{field}'")
    return value
