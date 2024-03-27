{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(durationid) IN ("NONE", ""), NULL, UPPER(durationid)) AS duration_id,
    IF(UPPER(durationcode) IN ("NONE", ""), NULL, UPPER(durationcode)) AS duration_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,
    IF(UPPER(durationinbangla) IN ("NONE", ""), NULL, UPPER(durationinbangla)) AS duration_in_bangla,
    IF(UPPER(durationinenglish) IN ("NONE", ""), NULL, UPPER(durationinenglish)) AS duration_in_english

FROM
    {{ ref("lan_dbo_refduration") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY duration_id ORDER BY updated_at DESC) = 1
