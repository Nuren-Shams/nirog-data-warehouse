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
    , dm_diagnosed_patients
    , non_dm_patients
    , medication_received_patients
    , lost_followup_patients

FROM 
    {{ ref("stg_agg_dm_info_daily") }}