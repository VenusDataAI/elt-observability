"""Lazy-loading singleton for config.yaml."""

from __future__ import annotations

import pathlib
from typing import Any

import yaml

_config: dict[str, Any] | None = None


def get_config(path: str = "config.yaml") -> dict[str, Any]:
    """Return the loaded config dict, loading from *path* on first call."""
    global _config
    if _config is None:
        _config = yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
    return _config


def reset_config() -> None:
    """Reset the singleton — use only in tests."""
    global _config
    _config = None
