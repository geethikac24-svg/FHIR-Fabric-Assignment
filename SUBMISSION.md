# FHIR Lakehouse Submission Package

This ZIP/repo contains a complete Medallion Lakehouse solution for incremental
FHIR API ingestion (Patient, Encounter, Observation, Condition).

## How to run locally

```powershell
cd fhir-lakehouse
python -m pip install -r requirements.txt
$env:PYTHONPATH = "src"
python -m fhir_lakehouse.pipeline
```

## How to deploy to Microsoft Fabric

1. Create a Lakehouse and upload `src/fhir_lakehouse` to `Files/libs/`.
2. Import notebooks from `notebooks/*.ipynb`.
3. Create a Data Pipeline using activity order in `pipelines/fabric_pipeline.json`.
4. Schedule daily.

Full docs: README.md, docs/architecture.md, docs/table-relationships.md, docs/pipeline-guide.md
