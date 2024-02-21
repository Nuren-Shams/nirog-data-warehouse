{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(drugid) IN ("NONE", ""), NULL, UPPER(drugid)) AS drug_id,
    IF(UPPER(durationid) IN ("NONE", ""), NULL, UPPER(durationid)) AS duration_id,
    IF(UPPER(mdtreatmentsuggestionid) IN ("NONE", ""), NULL, UPPER(mdtreatmentsuggestionid)) AS md_treatment_suggestion_id,
    IF(UPPER(reffrequencyid) IN ("NONE", ""), NULL, UPPER(reffrequencyid)) AS ref_frequency_id,
    IF(UPPER(refinstructionid) IN ("NONE", ""), NULL, UPPER(refinstructionid)) AS ref_instruction_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(drugdose) IN ("NONE", ""), NULL, UPPER(drugdose)) AS drug_dose,
    IF(UPPER(frequency) IN ("NONE", ""), NULL, UPPER(frequency)) AS frequency,
    IF(UPPER(drugdurationvalue) IN ("NONE", ""), NULL, UPPER(drugdurationvalue)) AS drug_duration_value,
    IF(UPPER(otherdrug) IN ("NONE", ""), NULL, UPPER(otherdrug)) AS other_drug

FROM
    {{ ref("lan_dbo_mdatatreatmentsuggestion") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id, collected_date, md_treatment_suggestion_id  ORDER BY updated_at DESC) = 1
