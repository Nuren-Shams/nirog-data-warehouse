{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(drugid) IN ("NONE", ""), NULL, UPPER(drugid)) AS drug_id,
    IF(UPPER(druggroupid) IN ("NONE", ""), NULL, UPPER(druggroupid)) AS drug_group_id,
    IF(UPPER(drugformid) IN ("NONE", ""), NULL, UPPER(drugformid)) AS drug_form_id,
    IF(UPPER(drugcode) IN ("NONE", ""), NULL, UPPER(drugcode)) AS drug_code,
    IF(UPPER(drugdose) IN ("NONE", ""), NULL, UPPER(drugdose)) AS drug_dose,
    IF(UPPER(description) IN ("NONE", ""), NULL, UPPER(description)) AS description,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", createdate) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updatedate) AS updated_at,

FROM
    {{ ref("lan_dbo_refdrug") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY drug_id ORDER BY updated_at DESC) = 1
