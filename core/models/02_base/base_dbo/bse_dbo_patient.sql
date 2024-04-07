{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    IF(UPPER(registrationid) IN ("NONE", ""), NULL, UPPER(registrationid)) AS registration_id,
    IF(UPPER(idtype) IN ("NONE", ""), NULL, UPPER(idtype)) AS id_type,
    IF(UPPER(idnumber) IN ("NONE", ""), NULL, UPPER(idnumber)) AS id_number,
    IF(UPPER(patientcode) IN ("NONE", ""), NULL, UPPER(patientcode)) AS patient_code,
    IF(UPPER(givenname) IN ("NONE", ""), NULL, givenname) AS given_name,
    IF(UPPER(familyname) IN ("NONE", ""), NULL, familyname) AS family_name,
    IF(UPPER(spousename) IN ("NONE", ""), NULL, spousename) AS spouse_name,
    IF(UPPER(fathername) IN ("NONE", ""), NULL, fathername) AS father_name,
    IF(UPPER(mothername) IN ("NONE", ""), NULL, mothername) AS mother_name,

    IF(UPPER(genderid) IN ("NONE", ""), NULL, genderid) AS gender_id,
    SAFE.PARSE_DATE("%Y-%m-%d", birthdate) AS birth_date,
    SAFE_CAST(age AS INT64) AS age,
    IF(UPPER(cellnumber) IN ("NONE", ""), NULL, cellnumber) AS cell_number,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", joiningdate) AS joined_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(workplaceid) IN ("NONE", ""), NULL, UPPER(workplaceid)) AS workplace_id,
    IF(UPPER(workplacebranchid) IN ("NONE", ""), NULL, UPPER(workplacebranchid)) AS workplace_branch_id,
    IF(UPPER(barcode) IN ("NONE", ""), NULL, UPPER(barcode)) AS barcode

FROM
    {{ ref("lan_dbo_patient") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY updated_at DESC) = 1
