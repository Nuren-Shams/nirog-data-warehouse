{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(id) IN ("NONE", ""), NULL, UPPER(id)) AS district_id,
    IF(UPPER(districtname) IN ("NONE", ""), NULL, UPPER(districtname)) AS district_name,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,
    IF(UPPER(divisionid) IN ("NONE", ""), NULL, UPPER(divisionid)) AS division_id,

FROM
    {{ ref("lan_dbo_district") }}
