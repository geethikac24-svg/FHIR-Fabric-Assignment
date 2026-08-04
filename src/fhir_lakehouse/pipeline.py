"""End-to-end medallion pipeline orchestrator.

Order: Patient → Encounter → Observation → Condition
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import get_resources_ordered, load_settings, resolve_lakehouse_path
from .fhir_client import FhirClient
from .metadata import append_run_log, raw_resource_dir, utc_now_iso, write_json
from .scd2 import apply_scd2, compute_version_hash
from .storage import read_parquet_table, table_stats, upsert_replace
from .transforms import bronze_to_silver_df, entries_to_bronze_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("fhir_pipeline")


def _client_from_cfg(cfg: dict[str, Any]) -> FhirClient:
    api = cfg["api"]
    return FhirClient(
        base_url=api["base_url"],
        page_size=api["page_size"],
        timeout=api["request_timeout_seconds"],
        max_retries=api["max_retries"],
        retry_backoff=api["retry_backoff_seconds"],
        accept=api.get("accept", "application/fhir+json"),
    )


def ingest_resource(
    cfg: dict[str, Any],
    resource: dict[str, Any],
    client: FhirClient,
    *,
    max_pages: int | None = None,
) -> dict[str, Any]:
    name = resource["name"]
    lh = cfg["lakehouse"]
    root = resolve_lakehouse_path(cfg)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    result = client.fetch_incremental(
        resource_name=name,
        resource_path=resource["path"],
        lookback_days=cfg["api"]["lookback_days"],
        max_pages=max_pages,
    )

    # --- Raw layer: store API responses as-is, bucketed by date ---
    raw_dir = raw_resource_dir(root, lh["raw_prefix"], name)
    for i, (url, bundle) in enumerate(zip(result.api_urls, result.bundles)):
        if isinstance(bundle, dict) and "_xml" in bundle:
            path = raw_dir / f"bundle_{run_id}_p{i:04d}.xml"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(bundle["_xml"], encoding="utf-8")
        else:
            path = raw_dir / f"bundle_{run_id}_p{i:04d}.json"
            write_json(
                path,
                {
                    "api_url": url,
                    "api_call_timestamp": result.api_call_timestamp,
                    "saved_timestamp": utc_now_iso(),
                    "bundle": bundle,
                },
            )

    api_url_summary = json.dumps(
        {"urls": result.api_urls[:3], "page_count": result.page_count, "params": result.params}
    )

    # --- Bronze: flatten + metadata ---
    bronze_rows = entries_to_bronze_records(
        name,
        result.entries,
        api_url_or_params=api_url_summary,
        api_call_timestamp=result.api_call_timestamp,
    )
    bronze_df = pd.DataFrame(bronze_rows)
    bronze_path = root / lh["bronze_prefix"] / name.lower()
    # Append-friendly: write dated partition file
    bronze_path.mkdir(parents=True, exist_ok=True)
    part_file = bronze_path / f"ingest_{run_id}.parquet"
    if not bronze_df.empty:
        bronze_df.to_parquet(part_file, index=False)

    # --- Silver: clean/dedupe + SCD2 history ---
    silver_clean = bronze_to_silver_df(bronze_df)
    if not silver_clean.empty:
        exclude = cfg.get("scd2", {}).get("hash_columns_exclude", [])
        silver_clean["version_hash"] = silver_clean.apply(
            lambda r: compute_version_hash(r.to_dict(), exclude), axis=1
        )

    silver_hist_path = root / lh["silver_prefix"] / f"{name.lower()}_scd2"
    existing = read_parquet_table(silver_hist_path)
    silver_scd2 = apply_scd2(
        existing,
        silver_clean,
        primary_key="resource_id",
        exclude_from_hash=cfg.get("scd2", {}).get("hash_columns_exclude"),
    )
    upsert_replace(silver_scd2, silver_hist_path)

    # Current-only silver view table
    silver_current = (
        silver_scd2[silver_scd2["is_current"] == True].copy()  # noqa: E712
        if not silver_scd2.empty
        else silver_scd2
    )
    upsert_replace(silver_current, root / lh["silver_prefix"] / name.lower())

    stats = {
        "resource": name,
        "run_id": run_id,
        "raw_pages": result.page_count,
        "raw_entries": result.entry_count,
        "bronze": table_stats(bronze_df),
        "silver_current": table_stats(silver_current),
        "silver_scd2": table_stats(silver_scd2),
        "api_call_timestamp": result.api_call_timestamp,
        "saved_timestamp": utc_now_iso(),
    }
    append_run_log(root / lh["logs_prefix"] / "ingestion_log.jsonl", stats)
    logger.info("Completed %s: %s", name, stats)
    return stats


def build_gold(cfg: dict[str, Any]) -> dict[str, Any]:
    """Create Gold analytics tables from Silver current snapshots."""
    root = resolve_lakehouse_path(cfg)
    lh = cfg["lakehouse"]
    silver = root / lh["silver_prefix"]
    gold = root / lh["gold_prefix"]
    gold.mkdir(parents=True, exist_ok=True)

    patients = read_parquet_table(silver / "patient")
    encounters = read_parquet_table(silver / "encounter")
    observations = read_parquet_table(silver / "observation")
    conditions = read_parquet_table(silver / "condition")

    # dim_patient
    dim_patient = patients.copy()
    if not dim_patient.empty:
        keep = [
            c
            for c in [
                "resource_id",
                "gender",
                "birth_date",
                "full_name",
                "family_name",
                "given_name",
                "city",
                "state",
                "country",
                "active",
                "fhir_last_updated",
                "effective_from",
                "is_current",
                "version_hash",
            ]
            if c in dim_patient.columns
        ]
        dim_patient = dim_patient[keep].rename(columns={"resource_id": "patient_id"})
    upsert_replace(dim_patient, gold / "dim_patient")

    # dim_condition_code
    if not conditions.empty:
        dim_cond = (
            conditions[["code_system", "code", "code_display"]]
            .dropna(subset=["code"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_cond.insert(0, "condition_code_sk", range(1, len(dim_cond) + 1))
    else:
        dim_cond = pd.DataFrame(columns=["condition_code_sk", "code_system", "code", "code_display"])
    upsert_replace(dim_cond, gold / "dim_condition_code")

    # fact_encounter
    fact_enc = encounters.copy()
    if not fact_enc.empty:
        cols = [
            c
            for c in [
                "resource_id",
                "patient_id",
                "status",
                "class_code",
                "type_code",
                "type_display",
                "period_start",
                "period_end",
                "fhir_last_updated",
            ]
            if c in fact_enc.columns
        ]
        fact_enc = fact_enc[cols].rename(columns={"resource_id": "encounter_id"})
    upsert_replace(fact_enc, gold / "fact_encounter")

    # fact_observation
    fact_obs = observations.copy()
    if not fact_obs.empty:
        cols = [
            c
            for c in [
                "resource_id",
                "patient_id",
                "encounter_id",
                "status",
                "code",
                "code_display",
                "effective_datetime",
                "value",
                "value_unit",
                "value_type",
                "fhir_last_updated",
            ]
            if c in fact_obs.columns
        ]
        fact_obs = fact_obs[cols].rename(columns={"resource_id": "observation_id"})
    upsert_replace(fact_obs, gold / "fact_observation")

    # fact_condition
    fact_cond = conditions.copy()
    if not fact_cond.empty:
        cols = [
            c
            for c in [
                "resource_id",
                "patient_id",
                "encounter_id",
                "clinical_status",
                "code",
                "code_display",
                "onset_datetime",
                "recorded_date",
                "fhir_last_updated",
            ]
            if c in fact_cond.columns
        ]
        fact_cond = fact_cond[cols].rename(columns={"resource_id": "condition_id"})
    upsert_replace(fact_cond, gold / "fact_condition")

    # gold_patient_summary — reporting-ready aggregate
    summary_rows: list[dict[str, Any]] = []
    if not patients.empty:
        enc_counts = (
            encounters.groupby("patient_id").size().rename("encounter_count")
            if not encounters.empty and "patient_id" in encounters.columns
            else pd.Series(dtype=int)
        )
        obs_counts = (
            observations.groupby("patient_id").size().rename("observation_count")
            if not observations.empty and "patient_id" in observations.columns
            else pd.Series(dtype=int)
        )
        cond_counts = (
            conditions.groupby("patient_id").size().rename("condition_count")
            if not conditions.empty and "patient_id" in conditions.columns
            else pd.Series(dtype=int)
        )
        base = patients[["resource_id", "full_name", "gender", "birth_date", "city", "state"]].rename(
            columns={"resource_id": "patient_id"}
        )
        summary = base.set_index("patient_id")
        for s in (enc_counts, obs_counts, cond_counts):
            if len(s):
                summary = summary.join(s, how="left")
        for col in ["encounter_count", "observation_count", "condition_count"]:
            if col not in summary.columns:
                summary[col] = 0
            summary[col] = summary[col].fillna(0).astype(int)
        summary = summary.reset_index()
        summary["built_at"] = utc_now_iso()
        summary_rows = summary.to_dict(orient="records")
        upsert_replace(summary, gold / "gold_patient_summary")
    else:
        upsert_replace(pd.DataFrame(), gold / "gold_patient_summary")

    stats = {
        "dim_patient": len(dim_patient),
        "dim_condition_code": len(dim_cond),
        "fact_encounter": len(fact_enc),
        "fact_observation": len(fact_obs),
        "fact_condition": len(fact_cond),
        "gold_patient_summary": len(summary_rows),
    }
    append_run_log(root / lh["logs_prefix"] / "gold_log.jsonl", {"built_at": utc_now_iso(), **stats})
    logger.info("Gold build complete: %s", stats)
    return stats


def run_pipeline(*, max_pages: int | None = None, config_path: str | None = None) -> dict[str, Any]:
    cfg = load_settings(config_path)
    client = _client_from_cfg(cfg)
    resources = get_resources_ordered(cfg)

    logger.info(
        "Starting pipeline: resources=%s lookback_days=%s order=%s",
        [r["name"] for r in resources],
        cfg["api"]["lookback_days"],
        " → ".join(r["name"] for r in resources),
    )

    resource_stats = []
    for resource in resources:
        resource_stats.append(ingest_resource(cfg, resource, client, max_pages=max_pages))

    gold_stats = build_gold(cfg)
    summary = {
        "pipeline": "fhir_medallion",
        "completed_at": utc_now_iso(),
        "resources": resource_stats,
        "gold": gold_stats,
    }
    root = resolve_lakehouse_path(cfg)
    write_json(root / cfg["lakehouse"]["logs_prefix"] / "last_pipeline_run.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="FHIR Medallion Lakehouse Pipeline")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages per resource (dev)")
    parser.add_argument("--config", type=str, default=None, help="Path to settings.yaml")
    args = parser.parse_args(argv)

    run_pipeline(max_pages=args.max_pages, config_path=args.config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
