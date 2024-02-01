{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "dm"]
    )
-}}

SELECT
    period_start_date
    , health_center_name
    , district_name
    , upazila_name
    , union_name 
    , "" AS period
    , registered_patients
    , dm_screened_patients
    , non_dm_patients
    , medication_received_patients
    , lost_followup_patients
    , cumulative_registered_patients
    , cumulative_dm_screened_patients
    , cumulative_non_dm_patients
    , cumulative_medication_received_patients
    , cumulative_lost_followup_patients

FROM 
    {{ ref("stg_agg_dm_info_daily") }}