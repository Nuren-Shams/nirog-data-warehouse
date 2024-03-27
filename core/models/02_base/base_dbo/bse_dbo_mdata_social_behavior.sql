{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdsocialbehaviorid) IN ("NONE", ""), NULL, UPPER(mdsocialbehaviorid)) AS md_social_behavior_id,
    IF(UPPER(socialbehaviorid) IN ("NONE", ""), NULL, UPPER(socialbehaviorid)) AS social_behavior_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatasocialbehavior") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, social_behavior_id ORDER BY updated_at DESC) = 1
