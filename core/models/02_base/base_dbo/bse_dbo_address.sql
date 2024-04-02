{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(addressid) IN ("NONE", ""), NULL, UPPER(addressid)) AS address_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(addressline1) IN ("NONE", ""), NULL, UPPER(addressline1)) AS address_line_1,
    IF(UPPER(addressline2) IN ("NONE", ""), NULL, UPPER(addressline2)) AS address_line_2,
    IF(UPPER(blocknumber) IN ("NONE", ""), NULL, UPPER(blocknumber)) AS block_number,
    IF(UPPER(camp) IN ("NONE", ""), NULL, UPPER(camp)) AS camp,
    IF(UPPER(country) IN ("NONE", ""), NULL, UPPER(country)) AS country,
    IF(UPPER(postcode) IN ("NONE", ""), NULL, UPPER(postcode)) AS post_code,
    IF(UPPER(village) IN ("NONE", ""), NULL, UPPER(village)) AS village,

FROM
    {{ ref("lan_dbo_address") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY updated_at DESC) = 1
