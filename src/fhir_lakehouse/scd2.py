"""Slowly Changing Dimension Type 2 (SCD2) utilities."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd


def _canonicalize(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, float) and value != value:  # NaN
            return None
    except Exception:
        pass
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return value


def compute_version_hash(row: dict[str, Any], exclude: Iterable[str] | None = None) -> str:
    exclude_set = set(exclude or [])
    payload = {}
    for k, v in sorted(row.items()):
        if k in exclude_set:
            continue
        canon = _canonicalize(v)
        if canon is None:
            continue
        payload[k] = canon
    blob = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def apply_scd2(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    *,
    primary_key: str = "resource_id",
    hash_col: str = "version_hash",
    exclude_from_hash: Iterable[str] | None = None,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    """
    Merge incoming rows into an SCD2 history table.

    - New keys → insert with is_current=True
    - Existing key, same hash → no change (keep current)
    - Existing key, different hash → expire current (effective_to, is_current=False)
      and insert new current version
    """
    as_of = as_of or datetime.now(timezone.utc)
    as_of_iso = as_of.isoformat()
    exclude = list(
        exclude_from_hash
        or [
            "extraction_timestamp",
            "api_url_or_params",
            "raw_json",
            "effective_from",
            "effective_to",
            "is_current",
            "version_hash",
            "load_timestamp",
            "api_call_timestamp",
            "saved_timestamp",
        ]
    )

    if incoming.empty:
        return existing.copy() if existing is not None else incoming.copy()

    inc = incoming.copy()
    if hash_col not in inc.columns:
        inc[hash_col] = inc.apply(
            lambda r: compute_version_hash(r.to_dict(), exclude), axis=1
        )

    if existing is None or existing.empty:
        out = inc.copy()
        out["effective_from"] = as_of_iso
        out["effective_to"] = None
        out["is_current"] = True
        return out.reset_index(drop=True)

    hist = existing.copy()
    # Ensure SCD2 columns exist
    for col, default in [
        ("effective_from", as_of_iso),
        ("effective_to", None),
        ("is_current", True),
        (hash_col, ""),
    ]:
        if col not in hist.columns:
            hist[col] = default

    current = hist[hist["is_current"] == True]  # noqa: E712
    current_by_key = current.set_index(primary_key, drop=False)

    expired_rows: list[pd.Series] = []
    new_rows: list[dict[str, Any]] = []
    unchanged_keys: set[Any] = set()

    for _, row in inc.iterrows():
        key = row[primary_key]
        row_dict = row.to_dict()
        new_hash = row_dict.get(hash_col) or compute_version_hash(row_dict, exclude)
        row_dict[hash_col] = new_hash

        if key not in current_by_key.index:
            row_dict["effective_from"] = as_of_iso
            row_dict["effective_to"] = None
            row_dict["is_current"] = True
            new_rows.append(row_dict)
            continue

        cur = current_by_key.loc[key]
        if isinstance(cur, pd.DataFrame):
            cur = cur.iloc[0]
        old_hash = cur.get(hash_col)
        if old_hash == new_hash:
            unchanged_keys.add(key)
            continue

        # Expire current version
        expired = cur.copy()
        expired["effective_to"] = as_of_iso
        expired["is_current"] = False
        expired_rows.append(expired)

        row_dict["effective_from"] = as_of_iso
        row_dict["effective_to"] = None
        row_dict["is_current"] = True
        new_rows.append(row_dict)

    # Rebuild history: non-current + current that weren't expired + new versions
    expired_keys = {r[primary_key] for r in expired_rows}
    keep_mask = ~(
        (hist["is_current"] == True)  # noqa: E712
        & hist[primary_key].isin(expired_keys)
    )
    kept = hist[keep_mask].copy()

    frames = [kept]
    if expired_rows:
        frames.append(pd.DataFrame(expired_rows))
    if new_rows:
        frames.append(pd.DataFrame(new_rows))

    result = pd.concat(frames, ignore_index=True)
    # Deduplicate identical historical rows if any
    return result.reset_index(drop=True)
