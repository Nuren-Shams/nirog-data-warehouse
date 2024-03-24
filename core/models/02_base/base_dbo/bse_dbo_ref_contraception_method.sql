{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(contraceptionmethodid) IN ("NONE", ""), NULL, UPPER(contraceptionmethodid)) AS contraception_method_id,
    IF(UPPER(contraceptionmethodcode) IN ("NONE", ""), NULL, UPPER(contraceptionmethodcode)) AS contraception_method_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refcontraceptionmethod") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY contraception_method_id ORDER BY updated_at DESC) = 1
