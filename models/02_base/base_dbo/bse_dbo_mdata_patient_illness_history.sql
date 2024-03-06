{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(illnessid) IN ("NONE", ""), NULL, UPPER(illnessid)) AS illness_id,
    IF(UPPER(mdpatientillnessid) IN ("NONE", ""), NULL, UPPER(mdpatientillnessid)) AS md_patient_illness_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(status) IN ("NONE", ""), NULL, UPPER(status)) AS illness_status,

FROM
    {{ ref("lan_dbo_mdatapatientillnesshistory") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, collected_date, illness_id ORDER BY updated_at DESC) = 1
