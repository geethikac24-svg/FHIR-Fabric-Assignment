# Table relationships

```
                    ┌─────────────────────┐
                    │  gold_dim_patient   │
                    │  PK: patient_id     │
                    └──────────┬──────────┘
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
┌──────────────────┐ ┌────────────────────┐ ┌──────────────────┐
│ gold_fact_       │ │ gold_fact_         │ │ gold_fact_       │
│ encounter        │ │ observation        │ │ condition        │
│ PK: encounter_id │ │ PK: observation_id │ │ PK: condition_id │
│ FK: patient_id   │ │ FK: patient_id     │ │ FK: patient_id   │
└────────┬─────────┘ │ FK: encounter_id   │ │ FK: encounter_id │
         │           └────────────────────┘ └────────┬─────────┘
         │                                           │
         │                                  ┌────────▼──────────┐
         │                                  │ gold_dim_         │
         │                                  │ condition_code    │
         │                                  │ SK: condition_    │
         │                                  │     code_sk       │
         │                                  │ BK: code_system+  │
         │                                  │     code          │
         │                                  └───────────────────┘
         └────────────────────────────────────────────┘
              (logical FK via encounter_id)
```

## Layer inventory

### Raw (Files)

```
raw/fhir/patient/YYYY/MM/DD/bundle_*.json
raw/fhir/encounter/YYYY/MM/DD/bundle_*.json
raw/fhir/observation/YYYY/MM/DD/bundle_*.json
raw/fhir/condition/YYYY/MM/DD/bundle_*.json
```

### Bronze tables

| Table | Grain | Key metadata |
|-------|-------|--------------|
| `bronze_patient` | 1 row / Patient resource / ingest | `resource_id`, `raw_json`, timestamps |
| `bronze_encounter` | 1 row / Encounter | + `patient_id` |
| `bronze_observation` | 1 row / Observation | + `patient_id`, `encounter_id` |
| `bronze_condition` | 1 row / Condition | + `patient_id`, `encounter_id` |

### Silver tables

| Table | Description |
|-------|-------------|
| `silver_{resource}` | Current cleaned row per `resource_id` |
| `silver_{resource}_scd2` | Full history with `is_current`, `effective_from/to`, `version_hash` |

### Gold tables / views

| Object | Type | Role |
|--------|------|------|
| `gold_dim_patient` | table | Patient dimension |
| `gold_dim_condition_code` | table | Condition code dimension |
| `gold_fact_encounter` | table | Encounter facts |
| `gold_fact_observation` | table | Observation facts |
| `gold_fact_condition` | table | Condition facts |
| `gold_vw_patient_summary` | view | Reporting rollup |
| `gold_vw_encounters_by_patient` | view | Encounter detail |
| `gold_vw_conditions_by_patient` | view | Condition detail |
| `gold_vw_observations_numeric` | view | Numeric labs |

### Metadata

| Object | Purpose |
|--------|---------|
| `meta_ingestion_runs` | API call + save timestamps per resource run |
| `Files/logs/ingestion_log.jsonl` | Append-only run log |

## Power BI (optional)

Connect to Lakehouse SQL analytics endpoint → start with `gold_vw_patient_summary`:

- Card: distinct patients
- Bar: encounters / observations / conditions by gender or state
- Table: top patients by observation_count
