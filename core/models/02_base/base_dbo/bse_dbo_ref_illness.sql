{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(illnessid) IN ("NONE", ""), NULL, UPPER(illnessid)) AS illness_id,
    REPLACE(
        REPLACE(
            IF(UPPER(illnesscode) IN ("NONE", ""), NULL, UPPER(illnesscode)), " ", "_"
        ), "/", "_"
    ) AS illness_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_refillness") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY illness_id ORDER BY updated_at DESC) = 1
