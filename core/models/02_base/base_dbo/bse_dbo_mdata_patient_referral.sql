{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    IF(UPPER(mdpatientreferralid) IN ("NONE", ""), NULL, UPPER(mdpatientreferralid)) AS md_patient_referral_id,
    IF(UPPER(healthcenterid) IN ("NONE", ""), NULL, UPPER(healthcenterid)) AS health_center_id,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,
    IF(UPPER(rid) IN ("NONE", ""), NULL, UPPER(rid)) AS r_id,

    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatapatientreferral") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date ORDER BY updated_at DESC) = 1
