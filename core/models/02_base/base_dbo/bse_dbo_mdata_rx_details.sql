{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(rxid) IN ("NONE", ""), NULL, UPPER(rxid)) AS rx_id,
    IF(UPPER(drugid) IN ("NONE", ""), NULL, UPPER(drugid)) AS drug_id,
    IF(UPPER(durationid) IN ("NONE", ""), NULL, UPPER(durationid)) AS duration_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    IF(UPPER(rx) IN ("NONE", ""), NULL, UPPER(rx)) AS rx_name,
    IF(UPPER(dose) IN ("NONE", ""), NULL, UPPER(dose)) AS rx_dose,
    IF(UPPER(frequencyhour) IN ("NONE", ""), NULL, UPPER(frequencyhour)) AS rx_frequency_hour,
    IF(UPPER(rxdurationvalue) IN ("NONE", ""), NULL, UPPER(rxdurationvalue)) AS rx_duration_value,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at

FROM
    {{ ref("lan_dbo_mdatarxdetails") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, rx_id ORDER BY updated_at DESC) = 1
