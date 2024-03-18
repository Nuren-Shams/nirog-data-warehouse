{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(prescriptioncreationid) IN ("NONE", ""), NULL, UPPER(prescriptioncreationid)) AS prescription_creation_id,
    IF(UPPER(prescriptionid) IN ("NONE", ""), NULL, UPPER(prescriptionid)) AS prescription_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    IF(UPPER(employeeid) IN ("NONE", ""), NULL, UPPER(employeeid)) AS employee_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate)) AS created_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_prescriptioncreation") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY prescription_creation_id ORDER BY updated_at DESC) = 1
