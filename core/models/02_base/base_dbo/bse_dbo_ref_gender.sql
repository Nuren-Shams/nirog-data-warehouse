{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(genderid) IN ("NONE", ""), NULL, UPPER(genderid)) AS gender_id,
    IF(UPPER(gendercode) IN ("NONE", ""), NULL, UPPER(gendercode)) AS gender_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refgender") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY gender_id ORDER BY updated_at DESC) = 1
