{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    SAFE_CAST(id AS INT64) AS division_id_int,
    IF(UPPER(name) IN ("NONE", ""), NULL, UPPER(name)) AS division_name

FROM
    {{ ref("lan_dbo_divisions") }}
