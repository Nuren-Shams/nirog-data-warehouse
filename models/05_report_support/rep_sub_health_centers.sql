{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT DISTINCT
    health_center_name
FROM
    {{ ref("bse_dbo_health_center") }}

UNION DISTINCT

SELECT NULL AS health_center_name
