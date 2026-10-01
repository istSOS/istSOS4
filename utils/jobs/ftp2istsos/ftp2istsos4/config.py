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


def config_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on"):
            return True
        if normalized in ("0", "false", "no", "off"):
            return False
    return bool(value)


def overwrite_observations_enabled(config, item=None):
    if isinstance(item, dict):
        if "overwrite_observations" in item:
            return config_bool(item.get("overwrite_observations"))
        if "update" in item:
            return config_bool(item.get("update"))

    if "overwrite_observations" in config:
        return config_bool(config.get("overwrite_observations"))
    return config_bool(config.get("update", False))


def observation_mode(config, item=None):
    mode = None
    if isinstance(item, dict):
        mode = item.get("observation_mode")
    if mode is None:
        mode = config.get("observation_mode", "append")

    mode = str(mode).strip().lower()
    if mode not in ("append", "backfill"):
        raise ValueError(
            "observation_mode must be 'append' or 'backfill', "
            f"got {mode!r}"
        )
    return mode


def require_value(item, field):
    value = item.get(field)
    if value in (None, ""):
        name = item.get("type", "unnamed")
        raise ValueError(f"{name}: missing required field '{field}'")
    return value
