-- Gold warehouse views for Fabric Warehouse / SQL analytics endpoint
-- Run after gold tables exist (notebook 03) or against mirrored Lakehouse SQL endpoint.

CREATE OR REPLACE VIEW gold_vw_encounters_by_patient AS
SELECT
  p.patient_id,
  p.full_name,
  p.gender,
  e.encounter_id,
  e.status,
  e.class_code,
  e.type_display,
  e.period_start,
  e.period_end
FROM gold_dim_patient p
JOIN gold_fact_encounter e ON p.patient_id = e.patient_id;

CREATE OR REPLACE VIEW gold_vw_conditions_by_patient AS
SELECT
  p.patient_id,
  p.full_name,
  c.condition_id,
  c.clinical_status,
  c.code,
  c.code_display,
  c.onset_datetime,
  c.encounter_id
FROM gold_dim_patient p
JOIN gold_fact_condition c ON p.patient_id = c.patient_id;

CREATE OR REPLACE VIEW gold_vw_observations_numeric AS
SELECT
  o.observation_id,
  o.patient_id,
  o.encounter_id,
  o.code,
  o.code_display,
  TRY_CAST(o.value AS DOUBLE) AS value_num,
  o.value_unit,
  o.effective_datetime
FROM gold_fact_observation o
WHERE o.value_type = 'Quantity';

-- Relationship cheat-sheet (logical FKs)
-- gold_dim_patient.patient_id          <— gold_fact_encounter.patient_id
-- gold_dim_patient.patient_id          <— gold_fact_observation.patient_id
-- gold_dim_patient.patient_id          <— gold_fact_condition.patient_id
-- gold_fact_encounter.encounter_id     <— gold_fact_observation.encounter_id
-- gold_fact_encounter.encounter_id     <— gold_fact_condition.encounter_id
-- gold_dim_condition_code.code         <— gold_fact_condition.code
