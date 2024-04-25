{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "external", "report", "dm"]
    )
-}}

SELECT
    period_start_date,
    health_center_name,
    barcode_prefix,
    district_name,
    upazila_name,
    union_name,
    period,
    registered_patients,
    dm_diagnosed_patients,
    non_dm_patients,
    medication_received_patients,
    lost_followup_patients

FROM
    {{ ref("rep_dm_info_period_wise") }}

WHERE
    TRUE
    {{ mcr_dgfp_health_center_filter("`Health_Center_Name`") }}
