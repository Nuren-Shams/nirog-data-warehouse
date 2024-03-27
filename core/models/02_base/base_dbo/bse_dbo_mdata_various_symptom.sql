{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdvarioussymptomid) IN ("NONE", ""), NULL, UPPER(mdvarioussymptomid)) AS md_various_symptom_id,
    IF(UPPER(anemiaseverityid) IN ("NONE", ""), NULL, UPPER(anemiaseverityid)) AS anemia_severity_id,
    IF(UPPER(anemiaseverity) IN ("NONE", ""), NULL, UPPER(anemiaseverity)) AS anemia_severity,
    IF(UPPER(coughgreaterthanmonth) IN ("NONE", ""), NULL, UPPER(coughgreaterthanmonth)) AS cough_greater_than_month,
    IF(UPPER(lgerf) IN ("NONE", ""), NULL, UPPER(lgerf)) AS lgerf,
    IF(UPPER(nightsweat) IN ("NONE", ""), NULL, UPPER(nightsweat)) AS night_sweat,
    IF(UPPER(weightloss) IN ("NONE", ""), NULL, UPPER(lgerf)) AS weight_loss,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatavarioussymptom") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
