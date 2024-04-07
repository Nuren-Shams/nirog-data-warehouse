{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(ccid) IN ("NONE", ""), NULL, UPPER(ccid)) AS cc_id,
    IF(UPPER(durationid) IN ("NONE", ""), NULL, UPPER(durationid)) AS duration_id,
    IF(UPPER(mdccid) IN ("NONE", ""), NULL, UPPER(mdccid)) AS md_cc_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(chiefcomplain) IN ("NONE", ""), NULL, UPPER(chiefcomplain)) AS chief_complain,
    SAFE_CAST(ccdurationvalue AS FLOAT64) AS cc_duration_value,
    IF(UPPER(nature) IN ("NONE", ""), NULL, UPPER(nature)) AS nature

FROM
    {{ ref("lan_dbo_mdatapatientccdetails") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, cc_id ORDER BY updated_at DESC) = 1
