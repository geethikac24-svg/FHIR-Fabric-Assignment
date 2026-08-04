# Architecture

## Medallion layers

| Layer | Storage | Purpose |
|-------|---------|---------|
| **Raw** | Lakehouse Files | Exact API JSON/XML bundles, partitioned `YYYY/MM/DD` per resource |
| **Bronze** | Delta / Parquet | One row per FHIR resource + metadata; append-only ingest partitions |
| **Silver** | Delta / Parquet | Cleaned, deduplicated current snapshot + SCD2 history tables |
| **Gold** | Delta + SQL views | Star-schema dims/facts optimized for reporting |

## Ingestion flow

1. Build search URL: `{base}/{Resource}?_lastUpdated=ge{lookback}&_count={page_size}`
2. Follow Bundle `link.relation=next` until exhausted (pagination).
3. Persist each page under Raw with `api_url`, `api_call_timestamp`, `saved_timestamp`.
4. Flatten entries → Bronze with `extraction_timestamp` and `api_url_or_params`.
5. Silver cleans strings, drops null IDs, keeps latest `fhir_last_updated`.
6. SCD2 merges on `resource_id` using `version_hash`.
7. Gold rebuilds dimensions, facts, and summary view.

## Orchestration order

```
Patient → Encounter → Observation → Condition → Silver SCD2 → Gold → Metadata log
```

Encounters/Observations/Conditions reference Patients, so Patient loads first.

## Why not Dataflow Gen2 for JSON→table?

Assignment constraint: Gen2 is allowed only for transformations. Spark notebooks perform API fetch, pagination, and JSON-to-Delta conversion. Optional Gen2 can replace Silver cleaning if preferred.

## Local vs Fabric

| Concern | Local runner | Fabric |
|---------|--------------|--------|
| Engine | pandas + parquet | Spark + Delta |
| Paths | `data/` | `/lakehouse/default/Files` + Tables |
| Orchestration | `python -m fhir_lakehouse.pipeline` | Data Pipeline |
| SCD2 | `scd2.apply_scd2` | Delta MERGE in notebook 02 |
