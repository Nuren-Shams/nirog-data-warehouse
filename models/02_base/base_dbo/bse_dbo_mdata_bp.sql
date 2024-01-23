{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}


WITH mdata_bp AS (
    SELECT
        patientid AS patient_id,
        CAST(ROUND(SAFE_CAST(bpdiastolic1 AS FLOAT64)) AS INT64) AS bp_diastolic_1,
        CAST(ROUND(SAFE_CAST(bpdiastolic2 AS FLOAT64)) AS INT64) AS bp_diastolic_2,
        CAST(ROUND(SAFE_CAST(bpsystolic1 AS FLOAT64)) AS INT64) AS bp_systolic_1,
        CAST(ROUND(SAFE_CAST(bpsystolic2 AS FLOAT64)) AS INT64) AS bp_systolic_2,
        PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
        PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
        PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,

    FROM
        {{ ref("lan_dbo_mdatabp") }}
)

SELECT *

FROM
    mdata_bp

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, DATE(collected_at) ORDER BY updated_at DESC) = 1
