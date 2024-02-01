{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT DISTINCT
    union_name
FROM
    {{ ref("bse_dbo_unions") }}

UNION DISTINCT

SELECT NULL AS union_name
