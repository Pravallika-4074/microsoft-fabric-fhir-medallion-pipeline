-- The notebook creates these views automatically. This script is an optional
-- explicit SQL equivalent for teams managing Gold objects separately.
CREATE OR REPLACE VIEW gold_patient AS
SELECT resource_id AS patient_id, patient_gender, patient_birth_date,
       last_updated, resource_hash, resource_json
FROM silver_fhir_patient WHERE is_current = true;

CREATE OR REPLACE VIEW gold_encounter AS
SELECT resource_id AS encounter_id, status, encounter_class_code,
       subject_reference AS patient_reference, period_start, period_end,
       last_updated, resource_hash, resource_json
FROM silver_fhir_encounter WHERE is_current = true;

CREATE OR REPLACE VIEW gold_observation AS
SELECT resource_id AS observation_id, status, subject_reference AS patient_reference,
       encounter_reference, code_text, observation_value, observation_unit,
       effective_datetime, last_updated, resource_hash, resource_json
FROM silver_fhir_observation WHERE is_current = true;

CREATE OR REPLACE VIEW gold_condition AS
SELECT resource_id AS condition_id, subject_reference AS patient_reference,
       encounter_reference, code_text, condition_clinical_status,
       condition_verification_status, last_updated, resource_hash, resource_json
FROM silver_fhir_condition WHERE is_current = true;
