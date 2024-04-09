{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "physical_exam_general", "extended"]
    )
-}}

WITH mdata_physical_exam_general AS (
    SELECT
        patient_id,
        collected_date,
        CASE anemia_severity
            WHEN 1 THEN "ANEMIA SEVERITY LEVEL - 1"
            WHEN 2 THEN "ANEMIA SEVERITY LEVEL - 2"
            WHEN 3 THEN "ANEMIA SEVERITY LEVEL - 3"
        END AS anemia_severity_processed,
        CASE edema_severity
            WHEN 1 THEN "EDEMA SEVERITY LEVEL - 1"
            WHEN 2 THEN "EDEMA SEVERITY LEVEL - 2"
            WHEN 3 THEN "EDEMA SEVERITY LEVEL - 3"
        END AS edema_severity_processed,
        CASE jaundice_severity
            WHEN 1 THEN "JAUNDICE SEVERITY LEVEL - 1"
            WHEN 2 THEN "JAUNDICE SEVERITY LEVEL - 2"
            WHEN 3 THEN "JAUNDICE SEVERITY LEVEL - 3"
        END AS jaundice_severity_processed,
        IF(is_heart_with_nad, "HEART WITH NAD", NULL) AS is_heart_with_nad_processed,
        IF(is_lungs_with_nad, "LUNGS WITH NAD", NULL) AS is_lungs_with_nad_processed,
        IF(is_lymph_nodes_with_palpable, "LYMPH NODES WITH PALPABLE", NULL) AS is_lymph_nodes_with_palpable_processed

    FROM
        {{ ref("bse_dbo_mdata_physical_exam_general") }}
)

SELECT
    patient_id,
    collected_date,
    physical_exam_general

FROM
    mdata_physical_exam_general UNPIVOT (
    physical_exam_general FOR _ IN (
        anemia_severity_processed,
        edema_severity_processed,
        jaundice_severity_processed,
        is_heart_with_nad_processed,
        is_lungs_with_nad_processed,
        is_lymph_nodes_with_palpable_processed
    )
)
