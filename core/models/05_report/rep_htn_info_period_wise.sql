{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn"]
    )
-}}

SELECT
    period_start_date
    , health_center_name
    , barcode_prefix
    , district_name
    , upazila_name
    , union_name 
    , "" AS period
    , registered_patients
    , htn_diagnosed_patients
    , non_htn_patients
    , medication_received_patients
    , lost_followup_patients

FROM 
    {{ ref("stg_agg_htn_info_daily") }}