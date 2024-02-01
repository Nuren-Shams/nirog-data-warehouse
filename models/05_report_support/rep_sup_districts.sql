{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT DISTINCT
    district_name
FROM
    {{ ref("bse_dbo_districts") }}

UNION DISTINCT

SELECT "" AS district_name
