{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdprovisionaldiagnosisid) IN ("NONE", ""), NULL, UPPER(mdprovisionaldiagnosisid)) AS md_provisional_diagnosis_id,
    IF(UPPER(refdiseasegroupid) IN ("NONE", ""), NULL, UPPER(refdiseasegroupid)) AS ref_disease_group_id,
    IF(UPPER(refprovisionaldiagnosisid) IN ("NONE", ""), NULL, UPPER(refprovisionaldiagnosisid)) AS ref_provisional_diagnosis_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(provisionaldiagnosis) IN ("NONE", ""), NULL, UPPER(provisionaldiagnosis)) AS provisional_diagnosis,
    IF(UPPER(otherprovisionaldiagnosis) IN ("NONE", ""), NULL, UPPER(otherprovisionaldiagnosis)) AS other_provisional_diagnosis,
    IF(UPPER(diagnosisstatus) IN ("NONE", ""), NULL, UPPER(diagnosisstatus)) AS diagnosis_status

FROM
    {{ ref("lan_dbo_mdataprovisionaldiagnosis") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, md_provisional_diagnosis_id ORDER BY updated_at DESC) = 1
