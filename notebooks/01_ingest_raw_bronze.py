"""
# 01 — Incremental FHIR Ingestion (Raw + Bronze)

Orchestration order: **Patient → Encounter → Observation → Condition**

- Paginated `_lastUpdated` lookback (default 3 days)
- Stores raw JSON bundles under `Files/raw/fhir/...`
- Writes Delta bronze tables with metadata columns
"""

# CELL: code
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# --- Config (override via notebook params / env) ---
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
PAGE_SIZE = 50
LOOKBACK_DAYS = 3
MAX_PAGES = None  # set e.g. 5 for smoke tests
FILES_ROOT = Path("/lakehouse/default/Files")

RESOURCES = [
    {"name": "Patient", "path": "Patient", "order": 1},
    {"name": "Encounter", "path": "Encounter", "order": 2},
    {"name": "Observation", "path": "Observation", "order": 3},
    {"name": "Condition", "path": "Condition", "order": 4},
]

# Prefer shared library when uploaded to Lakehouse Files/libs
LIBS = FILES_ROOT / "libs"
if LIBS.exists():
    sys.path.insert(0, str(LIBS))

from fhir_lakehouse.config import load_settings
from fhir_lakehouse.fhir_client import FhirClient
from fhir_lakehouse.metadata import raw_resource_dir, utc_now_iso, write_json, append_run_log
from fhir_lakehouse.transforms import entries_to_bronze_records

# CELL: code
client = FhirClient(base_url=FHIR_BASE_URL, page_size=PAGE_SIZE)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def save_bronze_spark(resource_name: str, rows: list[dict]):
    if not rows:
        print(f"No rows for {resource_name}")
        return
    df = spark.createDataFrame(rows)
    table = f"bronze_{resource_name.lower()}"
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )
    print(f"Appended {len(rows)} rows → {table}")

# CELL: code
run_stats = []
for resource in sorted(RESOURCES, key=lambda r: r["order"]):
    name = resource["name"]
    result = client.fetch_incremental(
        resource_name=name,
        resource_path=resource["path"],
        lookback_days=LOOKBACK_DAYS,
        max_pages=MAX_PAGES,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_dir = raw_resource_dir(FILES_ROOT, "raw/fhir", name)
    for i, (url, bundle) in enumerate(zip(result.api_urls, result.bundles)):
        write_json(
            raw_dir / f"bundle_{run_id}_p{i:04d}.json",
            {
                "api_url": url,
                "api_call_timestamp": result.api_call_timestamp,
                "saved_timestamp": utc_now_iso(),
                "bundle": bundle,
            },
        )

    api_meta = json.dumps(
        {"urls": result.api_urls[:3], "page_count": result.page_count, "params": result.params}
    )
    rows = entries_to_bronze_records(
        name,
        result.entries,
        api_url_or_params=api_meta,
        api_call_timestamp=result.api_call_timestamp,
    )
    save_bronze_spark(name, rows)
    stats = {
        "resource": name,
        "pages": result.page_count,
        "entries": result.entry_count,
        "api_call_timestamp": result.api_call_timestamp,
        "saved_timestamp": utc_now_iso(),
    }
    run_stats.append(stats)
    append_run_log(FILES_ROOT / "logs" / "ingestion_log.jsonl", stats)

display(spark.createDataFrame(run_stats))
