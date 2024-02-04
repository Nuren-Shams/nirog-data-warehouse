{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn"]
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
    , htn_screened_patients
    , non_htn_patients
    , medication_received_patients
    , lost_followup_patients
    , cumulative_registered_patients
    , cumulative_htn_screened_patients
    , cumulative_non_htn_patients
    , cumulative_medication_received_patients
    , cumulative_lost_followup_patients

FROM 
    {{ ref("stg_agg_htn_info_daily") }}