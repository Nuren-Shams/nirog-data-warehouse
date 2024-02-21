{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(socialbehaviorid) IN ("NONE", ""), NULL, UPPER(socialbehaviorid)) AS social_behavior_id,
    IF(UPPER(socialbehaviorcode) IN ("NONE", ""), NULL, UPPER(socialbehaviorcode)) AS social_behavior_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refsocialbehavior") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY social_behavior_id ORDER BY updated_at DESC) = 1
