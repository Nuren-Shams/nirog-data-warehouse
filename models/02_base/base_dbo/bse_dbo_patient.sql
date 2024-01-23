{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    patientid AS patient_id,
    IF(UPPER(idtype) IN ("NONE", ""), NULL, UPPER(idtype)) AS id_type,
    IF(UPPER(idnumber) IN ("NONE", ""), NULL, UPPER(idnumber)) AS id_number,
    IF(UPPER(givenname) IN ("NONE", ""), NULL, givenname) AS given_name,
    IF(UPPER(familyname) IN ("NONE", ""), NULL, familyname) AS family_name,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", joiningdate) AS joined_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_patient") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY updated_at DESC) = 1