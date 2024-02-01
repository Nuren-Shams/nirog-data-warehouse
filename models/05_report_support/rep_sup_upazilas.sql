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

SELECT NULL AS upazila_name
