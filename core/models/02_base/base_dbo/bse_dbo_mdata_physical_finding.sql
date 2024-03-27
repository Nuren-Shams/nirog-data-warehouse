{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(mdphysicalfindingid) IN ("NONE", ""), NULL, UPPER(mdphysicalfindingid)) AS md_physical_finding_id,
    IF(UPPER(patientid) IN ("NONE", ""), NULL, UPPER(patientid)) AS patient_id,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate) AS collected_at,
    DATE(SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", collectiondate)) AS collected_date,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updatedate) AS updated_at,
    IF(UPPER(physicalfinding) IN ("NONE", ""), NULL, UPPER(physicalfinding)) AS physical_finding,
    IF(UPPER(status) IN ("NONE", ""), NULL, UPPER(status)) AS status

FROM
    {{ ref("lan_dbo_mdataphysicalfinding") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, collected_date, md_physical_finding_id ORDER BY updated_at DESC) = 1
