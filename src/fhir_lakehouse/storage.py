"""Storage helpers — Parquet tables for local/Fabric Files paths."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def read_parquet_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.is_dir():
        files = list(path.glob("**/*.parquet"))
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return pd.read_parquet(path)


def write_parquet_table(df: pd.DataFrame, path: Path, *, partition_cols: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if partition_cols:
        # Simple non-hive write: single file (Fabric notebooks use Delta; local uses parquet)
        path.mkdir(parents=True, exist_ok=True)
        out = path / "part-0000.parquet"
        df.to_parquet(out, index=False)
    else:
        if path.suffix != ".parquet":
            path.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path / "part-0000.parquet", index=False)
        else:
            df.to_parquet(path, index=False)


def upsert_replace(df: pd.DataFrame, path: Path) -> None:
    """Full replace write for a logical table directory."""
    if path.exists() and path.is_dir():
        for f in path.glob("*.parquet"):
            f.unlink()
    write_parquet_table(df, path)


def table_stats(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(len(df)),
        "columns": list(df.columns) if not df.empty else [],
        "current_rows": int(df["is_current"].sum()) if "is_current" in df.columns else int(len(df)),
    }
