{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(adviceid) IN ("NONE", ""), NULL, UPPER(adviceid)) AS advice_id,
    IF(UPPER(mdadviceid) IN ("NONE", ""), NULL, UPPER(mdadviceid)) AS md_advice_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdataadvice") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, advice_id ORDER BY updated_at DESC) = 1
