{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE_CAST(fbg AS FLOAT64) AS fbg,
    SAFE_CAST(rbg AS FLOAT64) AS rbg,
    SAFE_CAST(hemoglobin AS FLOAT64) AS hemoglobin,
    SAFE_CAST(hrsfromlasteat AS FLOAT64) AS hrs_from_last_eat,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdataglucosehb") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
