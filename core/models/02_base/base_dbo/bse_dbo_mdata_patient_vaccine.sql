{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdpatientvaccineid) IN ("NONE", ""), NULL, UPPER(mdpatientvaccineid)) AS md_patient_vaccine_id,
    IF(UPPER(vaccineid) IN ("NONE", ""), NULL, UPPER(vaccineid)) AS vaccine_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatapatientvaccine") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, vaccine_id ORDER BY updated_at DESC) = 1
