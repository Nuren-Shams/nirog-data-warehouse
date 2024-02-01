{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT DISTINCT
    upazila_name
FROM
    {{ ref("bse_dbo_upazilas") }}

UNION DISTINCT

SELECT "" AS upazila_name
