from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


CONFIG_FILE_ENV = "SMOPS_CONFIG_FILE"
DEFAULT_REGION_ENV = "SMOPS_DEFAULT_REGION"


def config_path() -> Path:
    override = os.environ.get(CONFIG_FILE_ENV)
    if override:
        return Path(override).expanduser()
    config_home = os.environ.get("XDG_CONFIG_HOME")
    if config_home:
        return Path(config_home).expanduser() / "smops" / "config.json"
    return Path.home() / ".config" / "smops" / "config.json"


def load_config() -> dict[str, Any]:
    path = config_path()
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid smops config file: {path}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"Invalid smops config file: {path}")
    return loaded


def save_config(config: dict[str, Any]) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def get_default_region() -> str | None:
    env_region = os.environ.get(DEFAULT_REGION_ENV)
    if env_region:
        return env_region
    value = load_config().get("default_region")
    return value if isinstance(value, str) and value else None


def set_default_region(region: str) -> None:
    config = load_config()
    config["default_region"] = region
    save_config(config)


def resolve_region(region: str | None) -> str | None:
    return region or get_default_region()
