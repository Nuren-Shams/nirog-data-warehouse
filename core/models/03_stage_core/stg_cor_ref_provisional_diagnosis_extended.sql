{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "ref_provisional_diagnosis"]
    )
-}}

SELECT
    
FROM
    {{ ref("bse_dbo_ref_provisional_diagnosis") }}