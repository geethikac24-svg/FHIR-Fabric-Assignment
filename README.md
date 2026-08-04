# FHIR API Data Ingestion & Analytics

Medallion Lakehouse solution for incremental FHIR R4 ingestion from the public HAPI FHIR API.

**API:** https://hapi.fhir.org/baseR4/swagger-ui/  
**Resources:** Patient → Encounter → Observation → Condition  
**Layers:** Raw → Bronze → Silver (SCD2) → Gold  

## Architecture

```
HAPI FHIR API
    │  paginated _lastUpdated (2–3 day lookback)
    ▼
Raw (Files)          JSON/XML bundles bucketed by date
    │                 raw/fhir/{resource}/YYYY/MM/DD/
    ▼
Bronze (Delta)       Flattened rows + metadata columns
    │                 extraction_timestamp, api_url_or_params,
    │                 api_call_timestamp, saved_timestamp
    ▼
Silver (Delta)       Clean + dedupe + SCD Type 2 history
    │                 silver_{resource} (current)
    │                 silver_{resource}_scd2 (versions)
    ▼
Gold (Delta/Views)   dim_patient, fact_encounter,
                     fact_observation, fact_condition,
                     gold_vw_patient_summary
```

See [docs/architecture.md](docs/architecture.md) and [docs/table-relationships.md](docs/table-relationships.md).

## Quick start (local Python — proves ingestion)

```bash
cd fhir-lakehouse
python -m pip install -r requirements.txt
# Smoke test (2 pages/resource):
python -m fhir_lakehouse.pipeline --max-pages 2
# Full 3-day incremental load:
python -m fhir_lakehouse.pipeline
```

Outputs land under `data/raw`, `data/bronze`, `data/silver`, `data/gold`, `data/logs`.

Set `PYTHONPATH=src` if the module is not found:

```powershell
$env:PYTHONPATH = "src"
python -m fhir_lakehouse.pipeline --max-pages 2
```

## Microsoft Fabric setup

1. Create a **Lakehouse** (e.g. `fhir_lh`) in your Fabric workspace.
2. Upload `src/fhir_lakehouse/` to `Files/libs/fhir_lakehouse/`.
3. Import notebooks from `notebooks/` (`.py` cells marked `# CELL:` — convert to Fabric notebooks or paste into new notebooks).
4. Attach each notebook to the lakehouse.
5. Create a **Data Pipeline** using `pipelines/fabric_pipeline.json` as the activity order:
   - Setup → Ingest Raw/Bronze → Silver SCD2 → Gold → Metadata log
6. Schedule daily. Each run appends Bronze, merges SCD2, rebuilds Gold.

Optional: Gen2 Dataflows only for Silver cleaning — **not** for JSON→table conversion (Spark notebooks handle that).

## Databricks setup

1. Import notebooks under a `/fhir-lakehouse` folder.
2. Install `requirements.txt` on the cluster (or use `%pip`).
3. Create a Job from `pipelines/databricks_workflow.json`.
4. Point storage paths to your Unity Catalog / DBFS volume.

## Configuration (no hardcoding)

All tunables live in [`config/settings.yaml`](config/settings.yaml):

| Setting | Default | Env override |
|---------|---------|--------------|
| API base URL | `https://hapi.fhir.org/baseR4` | `FHIR_BASE_URL` |
| Page size | `50` | `FHIR_PAGE_SIZE` |
| Lookback days | `3` | `FHIR_LOOKBACK_DAYS` |
| Lakehouse root | `data` | `LAKEHOUSE_ROOT` |
| Accept header | `application/fhir+json` | `FHIR_ACCEPT` |

## Metadata & versioning

Every Bronze/Silver row includes:

- `extraction_timestamp` — when the row was extracted
- `api_url_or_params` — request URLs / search params
- `api_call_timestamp` — when the API was called
- `saved_timestamp` / `load_timestamp` — when persisted

SCD Type 2 columns on `silver_*_scd2`:

- `version_hash` — hash of business attributes
- `effective_from` / `effective_to`
- `is_current`

Daily loads compare hashes: unchanged → keep; changed → expire prior version + insert new.

## Submission checklist

- [x] Ingest 2–3 days with pagination  
- [x] Medallion Raw → Bronze → Silver → Gold  
- [x] Metadata + SCD2 versioning  
- [x] Modular reusable code (config-driven)  
- [x] Pipeline definition (Fabric + Databricks)  
- [x] Documentation of pipeline & table relationships  

## Project layout

```
fhir-lakehouse/
├── config/settings.yaml
├── src/fhir_lakehouse/          # reusable Python package
├── notebooks/                   # Fabric / Databricks notebooks
├── pipelines/                   # orchestration defs
├── sql/gold_views.sql
├── docs/
├── tests/
└── data/                        # local lakehouse root (gitignored contents)
```

## Optional extensions

- **XML:** set `FHIR_ACCEPT=application/fhir+xml` (raw XML stored; JSON path is primary).
- **Power BI:** connect to the Lakehouse SQL endpoint → `gold_vw_patient_summary` for a 1-page report (patients, encounter/obs/condition counts).
