{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "dm"]
    )
-}}

SELECT
    mdata.patient_id
    , p.given_name
    , p.family_name
    , p.created_at
    , p.id_number
    , p.id_type
    , mdata.collected_date
    , mdata.is_pregnant
    , p.workplace_id
    , p.workplace_name 
    , p.workplace_branch_code
    , p.district_name
    , mdata.fbg
    , mdata.rbg
    , mdata.prescribed_rx

FROM
    {{ ref("stg_cor_mdata_super_table") }} AS mdata

    LEFT JOIN {{ ref("stg_cor_patient_extended") }} AS p
        ON
            mdata.patient_id = p.patient_id

WHERE 
    TRUE
    AND fbg IS NOT NULL
