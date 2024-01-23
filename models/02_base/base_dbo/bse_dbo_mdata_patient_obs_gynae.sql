{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    patientid AS patient_id,
    UPPER(ispregnant) = "TRUE" AS is_pregnant,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatapatientobsgynae") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, DATE(collected_at) ORDER BY updated_at DESC) = 1
