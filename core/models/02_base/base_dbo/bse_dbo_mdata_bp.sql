{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE_CAST(ROUND(SAFE_CAST(bpdiastolic1 AS FLOAT64)) AS INT64) AS bp_diastolic_1,
    SAFE_CAST(ROUND(SAFE_CAST(bpdiastolic2 AS FLOAT64)) AS INT64) AS bp_diastolic_2,
    SAFE_CAST(ROUND(SAFE_CAST(bpsystolic1 AS FLOAT64)) AS INT64) AS bp_systolic_1,
    SAFE_CAST(ROUND(SAFE_CAST(bpsystolic2 AS FLOAT64)) AS INT64) AS bp_systolic_2,
    SAFE_CAST(ROUND(SAFE_CAST(heartrate AS FLOAT64)) AS INT64) AS heart_rate,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatabp") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
