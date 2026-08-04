"""
# 03 — Gold Layer: Analytics Warehouse Tables / Views

Builds reporting-ready dimensions and facts from Silver current tables.
"""

# CELL: code
from pyspark.sql import functions as F

# CELL: code
# dim_patient
spark.sql("""
CREATE OR REPLACE TABLE gold_dim_patient AS
SELECT
  resource_id AS patient_id,
  gender,
  birth_date,
  full_name,
  family_name,
  given_name,
  city,
  state,
  country,
  active,
  fhir_last_updated,
  version_hash,
  effective_from
FROM silver_patient
WHERE is_current = true OR is_current IS NULL
""")

# dim_condition_code
spark.sql("""
CREATE OR REPLACE TABLE gold_dim_condition_code AS
SELECT
  ROW_NUMBER() OVER (ORDER BY code_system, code) AS condition_code_sk,
  code_system,
  code,
  code_display
FROM (
  SELECT DISTINCT code_system, code, code_display
  FROM silver_condition
  WHERE code IS NOT NULL
)
""")

# fact_encounter
spark.sql("""
CREATE OR REPLACE TABLE gold_fact_encounter AS
SELECT
  resource_id AS encounter_id,
  patient_id,
  status,
  class_code,
  type_code,
  type_display,
  period_start,
  period_end,
  fhir_last_updated
FROM silver_encounter
""")

# fact_observation
spark.sql("""
CREATE OR REPLACE TABLE gold_fact_observation AS
SELECT
  resource_id AS observation_id,
  patient_id,
  encounter_id,
  status,
  code,
  code_display,
  effective_datetime,
  value,
  value_unit,
  value_type,
  fhir_last_updated
FROM silver_observation
""")

# fact_condition
spark.sql("""
CREATE OR REPLACE TABLE gold_fact_condition AS
SELECT
  resource_id AS condition_id,
  patient_id,
  encounter_id,
  clinical_status,
  code,
  code_display,
  onset_datetime,
  recorded_date,
  fhir_last_updated
FROM silver_condition
""")

# Reporting summary view
spark.sql("""
CREATE OR REPLACE VIEW gold_vw_patient_summary AS
SELECT
  p.patient_id,
  p.full_name,
  p.gender,
  p.birth_date,
  p.city,
  p.state,
  COALESCE(e.encounter_count, 0) AS encounter_count,
  COALESCE(o.observation_count, 0) AS observation_count,
  COALESCE(c.condition_count, 0) AS condition_count
FROM gold_dim_patient p
LEFT JOIN (
  SELECT patient_id, COUNT(*) AS encounter_count FROM gold_fact_encounter GROUP BY patient_id
) e ON p.patient_id = e.patient_id
LEFT JOIN (
  SELECT patient_id, COUNT(*) AS observation_count FROM gold_fact_observation GROUP BY patient_id
) o ON p.patient_id = o.patient_id
LEFT JOIN (
  SELECT patient_id, COUNT(*) AS condition_count FROM gold_fact_condition GROUP BY patient_id
) c ON p.patient_id = c.patient_id
""")

print("Gold tables/views ready")
display(spark.sql("SHOW TABLES LIKE 'gold_*'"))
display(spark.sql("SELECT * FROM gold_vw_patient_summary LIMIT 20"))
