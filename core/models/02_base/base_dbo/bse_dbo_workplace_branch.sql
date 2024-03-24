{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(workplacebranchid) IN ("NONE", ""), NULL, UPPER(workplacebranchid)) AS workplace_branch_id,
    IF(UPPER(workplaceid) IN ("NONE", ""), NULL, UPPER(workplaceid)) AS workplace_id,
    IF(UPPER(workplacebranchcode) IN ("NONE", ""), NULL, UPPER(workplacebranchcode)) AS workplace_branch_code,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,
    IF(UPPER(orgid) IN ("NONE", ""), NULL, UPPER(orgid)) AS org_id,

FROM
    {{ ref("lan_dbo_workplacebranch") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY workplace_branch_id ORDER BY updated_at DESC) = 1