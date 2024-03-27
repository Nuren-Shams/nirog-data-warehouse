{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "agg", "dm"]
    )
-}}

SELECT
    district_name,
    upazila_name,
    union_name,
    health_center_name,
    barcode_prefix,
    SUM(registered_patients) AS total_registered_patients,
    SUM(dm_diagnosed_patients) AS total_dm_diagnosed_patients,
    SUM(non_dm_patients) AS total_non_dm_patients,
    SUM(medication_received_patients) AS total_medication_received_patients,
    SUM(lost_followup_patients) AS total_lost_followup_patients

FROM
    {{ ref('stg_agg_dm_info_daily') }}

GROUP BY
    district_name,
    upazila_name,
    union_name,
    health_center_name,
    barcode_prefix
