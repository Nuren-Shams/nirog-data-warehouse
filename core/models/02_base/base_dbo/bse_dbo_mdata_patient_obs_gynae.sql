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
    IF(UPPER(childmortality0to1) IN ("NONE", ""), NULL, UPPER(childmortality0to1)) AS child_mortality_0_to_1,
    IF(UPPER(childmortalitybelow5) IN ("NONE", ""), NULL, UPPER(childmortalitybelow5)) AS child_mortality_below_5,
    IF(UPPER(childmortalityover5) IN ("NONE", ""), NULL, UPPER(childmortalityover5)) AS child_mortality_over_5,
    SAFE_CAST(gravida AS INT64) AS gravida,
    IF(UPPER(livingbirth) IN ("NONE", ""), NULL, UPPER(livingbirth)) AS living_birth,
    SAFE_CAST(livingfemale AS INT64) AS living_female,
    SAFE_CAST(livingmale AS INT64) AS living_male,
    SAFE.PARSE_DATE("%Y-%m-%d", lmp) AS lmp,
    SAFE_CAST(mr AS INT64) AS mr,
    SAFE_CAST(miscarraigeorabortion AS INT64) AS miscarraige_or_abortion,
    IF(UPPER(othercontraceptionmethod) IN ("NONE", ""), NULL, UPPER(othercontraceptionmethod)) AS other_contraception_method,
    IF(UPPER(othermenstruationproduct) IN ("NONE", ""), NULL, UPPER(othermenstruationproduct)) AS other_menstruation_product,
    IF(UPPER(othermenstruationproductusagetime) IN ("NONE", ""), NULL, UPPER(othermenstruationproductusagetime)) AS other_menstruation_product_usage_time,
    SAFE_CAST(stillbirth AS INT64) AS still_birth,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatapatientobsgynae") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
-- Removed contraception_method_id from dedup logic because it was creating multiple rows 
-- for patient_id, collected_date
