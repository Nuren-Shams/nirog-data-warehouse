{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(workplaceid) IN ("NONE", ""), NULL, UPPER(workplaceid)) AS workplace_id,
    IF(UPPER(workplacecode) IN ("NONE", ""), NULL, UPPER(workplacecode)) AS workplace_code,
    IF(UPPER(workplacename) IN ("NONE", ""), NULL, UPPER(workplacename)) AS workplace_name,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,
    IF(UPPER(districtid) IN ("NONE", ""), NULL, UPPER(districtid)) AS district_id

FROM
    {{ ref("lan_dbo_workplace") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY workplace_id ORDER BY updated_at DESC) = 1
