"""
# 04 — Metadata & Versioning Audit Log

Persists when each API was called and when data was saved.
"""

# CELL: code
from pyspark.sql import functions as F
from pathlib import Path
import json

FILES_ROOT = Path("/lakehouse/default/Files")
log_path = FILES_ROOT / "logs" / "ingestion_log.jsonl"

rows = []
if log_path.exists():
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))

if rows:
    df = spark.createDataFrame(rows)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("meta_ingestion_runs")
    )
    display(df.orderBy(F.col("saved_timestamp").desc()))
else:
    print("No ingestion logs yet — run 01_ingest_raw_bronze first.")

# Versioning summary across SCD2 tables
for resource in ["patient", "encounter", "observation", "condition"]:
    tbl = f"silver_{resource}_scd2"
    if spark.catalog.tableExists(tbl):
        display(
            spark.sql(
                f"""
                SELECT '{resource}' AS resource,
                       COUNT(*) AS total_versions,
                       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
                       SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_rows
                FROM {tbl}
                """
            )
        )
