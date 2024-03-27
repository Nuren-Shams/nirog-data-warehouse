{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(healthcentercode) IN ("NONE", ""), NULL, UPPER(healthcentercode)) AS health_center_code,
    IF(UPPER(healthcenterid) IN ("NONE", ""), NULL, UPPER(healthcenterid)) AS health_center_id,
    IF(UPPER(healthcentername) IN ("NONE", ""), NULL, UPPER(healthcentername)) AS health_center_name,
    IF(UPPER(healthcentertype) IN ("NONE", ""), NULL, UPPER(healthcentertype)) AS health_center_type,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_healthcenter") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY health_center_id ORDER BY updated_at DESC) = 1
