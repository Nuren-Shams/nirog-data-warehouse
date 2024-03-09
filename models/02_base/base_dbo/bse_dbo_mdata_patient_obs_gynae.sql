{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(contraceptionmethodid) IN ("NONE", ""), NULL, UPPER(contraceptionmethodid)) AS contraception_method_id,
    IF(UPPER(mdpatientobsgynaeid) IN ("NONE", ""), NULL, UPPER(mdpatientobsgynaeid)) AS md_patient_obsgynae_id,
    IF(UPPER(menstruationproductid) IN ("NONE", ""), NULL, UPPER(menstruationproductid)) AS menstruation_product_id,
    IF(UPPER(menstruationproductusagetimeid) IN ("NONE", ""), NULL, UPPER(menstruationproductusagetimeid)) AS menstruation_product_usage_time_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    UPPER(ispregnant) = "TRUE" AS is_pregnant,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatapatientobsgynae") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
-- Removed contraception_method_id from dedup logic because it was creating multiple rows 
-- for patient_id, collected_date