{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "patient", "extended"]
    )
-}}


SELECT 
    p.*
    , wp.workplace_code
    , wp.workplace_name
    , wp.district_id
    , d.district_name
FROM 
    {{ ref("bse_dbo_patient") }} AS p

    LEFT JOIN {{ ref("bse_dbo_workplace") }} AS wp
        ON 
            p.workplace_id = wp.workplace_id

    LEFT JOIN {{ ref("bse_dbo_district") }} AS d
        ON 
            wp.district_id = d.district_id
