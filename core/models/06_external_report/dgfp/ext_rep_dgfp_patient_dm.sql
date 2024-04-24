{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "external", "report", "dm"]
    )
-}}

SELECT
    patient_id,
    given_name,
    family_name,
    created_at,
    id_number,
    id_type,
    patient_code,
    collected_date,
    is_pregnant,
    health_center_name,
    district_name,
    upazila_name,
    union_name,
    fbg,
    rbg,
    prescribed_rx_names

FROM
    {{ ref("rep_patient_dm") }}

WHERE TRUE
    {{ mcr_dgfp_health_center_filter("`Health_Center_Name`") }}
