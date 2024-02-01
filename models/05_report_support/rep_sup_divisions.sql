{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT DISTINCT
    division_name
FROM
    {{ ref("bse_dbo_divisions") }}

UNION DISTINCT

SELECT "" AS division_name
