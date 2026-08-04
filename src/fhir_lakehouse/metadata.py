"""Metadata helpers and lakehouse path conventions."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def partition_date(dt: datetime | None = None) -> tuple[str, str, str]:
    d = dt or utc_now()
    return f"{d.year:04d}", f"{d.month:02d}", f"{d.day:02d}"


def raw_resource_dir(root: Path, raw_prefix: str, resource: str, dt: datetime | None = None) -> Path:
    y, m, d = partition_date(dt)
    return root / raw_prefix / resource.lower() / y / m / d


def add_metadata(
    record: dict[str, Any],
    *,
    api_url_or_params: str,
    extraction_timestamp: str | None = None,
    api_call_timestamp: str | None = None,
    saved_timestamp: str | None = None,
) -> dict[str, Any]:
    """Attach required metadata columns to a bronze/silver row."""
    out = dict(record)
    out["extraction_timestamp"] = extraction_timestamp or utc_now_iso()
    out["api_url_or_params"] = api_url_or_params
    out["api_call_timestamp"] = api_call_timestamp or out["extraction_timestamp"]
    out["saved_timestamp"] = saved_timestamp or utc_now_iso()
    out["load_timestamp"] = out["saved_timestamp"]
    return out


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def append_run_log(log_path: Path, event: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
