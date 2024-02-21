{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(id) IN ("NONE", ""), NULL, UPPER(id)) AS id,
    IF(UPPER(refbloodgroupid) IN ("NONE", ""), NULL, UPPER(refbloodgroupid)) AS ref_blood_group_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    SAFE_CAST(height AS FLOAT64) AS height,
    SAFE_CAST(weight AS FLOAT64) AS weight,
    SAFE_CAST(bmi AS FLOAT64) AS bmi

FROM
    {{ ref("lan_dbo_mdataheightweight") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
