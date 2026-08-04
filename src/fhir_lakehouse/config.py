"""Configuration loader with environment overrides (no hardcoding in notebooks)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Project root = fhir-lakehouse/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"


def _deep_get(d: dict, *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def load_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Environment overrides
    if os.getenv("FHIR_BASE_URL"):
        cfg["api"]["base_url"] = os.environ["FHIR_BASE_URL"].rstrip("/")
    if os.getenv("FHIR_PAGE_SIZE"):
        cfg["api"]["page_size"] = int(os.environ["FHIR_PAGE_SIZE"])
    if os.getenv("FHIR_LOOKBACK_DAYS"):
        cfg["api"]["lookback_days"] = int(os.environ["FHIR_LOOKBACK_DAYS"])
    if os.getenv("LAKEHOUSE_ROOT"):
        cfg["lakehouse"]["root"] = os.environ["LAKEHOUSE_ROOT"]
    if os.getenv("FHIR_ACCEPT"):
        cfg["api"]["accept"] = os.environ["FHIR_ACCEPT"]

    return cfg


def get_resources_ordered(cfg: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    cfg = cfg or load_settings()
    return sorted(cfg["resources"], key=lambda r: r["order"])


def resolve_lakehouse_path(cfg: dict[str, Any], *parts: str) -> Path:
    """Resolve a path under lakehouse root (local or mounted Files path)."""
    root = Path(cfg["lakehouse"]["root"])
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    return root.joinpath(*parts)
