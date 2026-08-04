# Pipeline guide

## Fabric Data Pipeline

1. Workspace → **New → Data Pipeline**.
2. Add **Notebook** activities matching `pipelines/fabric_pipeline.json`:
   1. `00_setup_lakehouse`
   2. `01_ingest_raw_bronze` (parameters: `LOOKBACK_DAYS=3`, `PAGE_SIZE=50`)
   3. `02_silver_scd2`
   4. `03_gold_analytics`
   5. `04_metadata_logging`
3. Wire dependencies Succeeded → next.
4. Attach the same Lakehouse to every notebook.
5. Schedule: daily after midnight UTC (aligns with `_lastUpdated` lookback window).

## Databricks Workflow

Import `pipelines/databricks_workflow.json` as a Job definition (or recreate tasks manually). Same order.

## Local CLI

```powershell
cd fhir-lakehouse
$env:PYTHONPATH = "src"
$env:FHIR_LOOKBACK_DAYS = "3"
python -m fhir_lakehouse.pipeline
```

## Failure handling

| Stage | On failure |
|-------|------------|
| Ingest | Retries per page (`max_retries`); pipeline stops resource; prior resources kept |
| Silver | Idempotent MERGE; re-run safe |
| Gold | Full rebuild from Silver current; re-run safe |

## Verification queries (Fabric)

```sql
SELECT resource_type, COUNT(*) FROM bronze_patient GROUP BY resource_type;
SELECT is_current, COUNT(*) FROM silver_patient_scd2 GROUP BY is_current;
SELECT * FROM gold_vw_patient_summary LIMIT 50;
SELECT * FROM meta_ingestion_runs ORDER BY saved_timestamp DESC;
```
