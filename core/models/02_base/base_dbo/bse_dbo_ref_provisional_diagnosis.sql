{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(refprovisionaldiagnosisid) IN ("NONE", ""), NULL, UPPER(refprovisionaldiagnosisid)) AS ref_provisional_diagnosis_id,
    IF(UPPER(refprovisionaldiagnosisgroupid) IN ("NONE", ""), NULL, UPPER(refprovisionaldiagnosisgroupid)) AS ref_provisional_diagnosis_group_id,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,
    TRIM(IF(UPPER(provisionaldiagnosiscode) IN ("NONE", ""), NULL, UPPER(provisionaldiagnosiscode))) AS provisional_diagnosis_code,
    TRIM(IF(UPPER(provisionaldiagnosisname) IN ("NONE", ""), NULL, UPPER(provisionaldiagnosisname))) AS provisional_diagnosis_name,
    TRIM(IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description))) AS description,

FROM
    {{ ref("lan_dbo_refprovisionaldiagnosis") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_provisional_diagnosis_id ORDER BY updated_at DESC) = 1
