{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(rxid) IN ("NONE", ""), NULL, UPPER(rxid)) AS rx_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    IF(UPPER(rx) IN ("NONE", ""), NULL, UPPER(rx)) AS rx_name,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_mdatarxdetails") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY rx_id ORDER BY updated_at DESC) = 1
