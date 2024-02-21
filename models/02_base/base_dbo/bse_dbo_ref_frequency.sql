{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(frequencyid) IN ("NONE", ""), NULL, UPPER(frequencyid)) AS frequency_id,
    IF(UPPER(frequencycode) IN ("NONE", ""), NULL, UPPER(frequencycode)) AS frequency_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    IF(UPPER(frequencyinbangla) IN ("NONE", ""), NULL, UPPER(frequencyinbangla)) AS frequency_in_bangla,
    IF(UPPER(frequencyinenglish) IN ("NONE", ""), NULL, UPPER(frequencyinenglish)) AS frequency_in_english,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_reffrequency") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY frequency_id ORDER BY updated_at DESC) = 1
