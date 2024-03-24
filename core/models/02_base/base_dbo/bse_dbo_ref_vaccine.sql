{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(vaccineid) IN ("NONE", ""), NULL, UPPER(vaccineid)) AS vaccine_id,
    IF(UPPER(vaccinecode) IN ("NONE", ""), NULL, UPPER(vaccinecode)) AS vaccine_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refvaccine") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY vaccine_id ORDER BY updated_at DESC) = 1
