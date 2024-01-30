{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn"]
    )
-}}

SELECT
    period_start_date
    , workplace_id
    , workplace_name
    , workplace_branch_code
    , district_name 
    , "DAILY" AS period
    , registered_patients
    , htn_screened_patients
    , non_htn_patients
    , lost_followup_patients
    , cumulative_registered_patients
    , cumulative_htn_screened_patients
    , cumulative_non_htn_patients
    , cumulative_lost_followup_patients
    
    , htn_screened_patients - lost_followup_patients AS med_received_patients

FROM 
    {{ ref("stg_agg_htn_info_daily") }}

UNION ALL 

SELECT
    period_start_date
    , workplace_id
    , workplace_name
    , workplace_branch_code
    , district_name 
    , "MONTHLY" AS period
    , registered_patients
    , htn_screened_patients
    , non_htn_patients
    , lost_followup_patients
    , cumulative_registered_patients
    , cumulative_htn_screened_patients
    , cumulative_non_htn_patients
    , cumulative_lost_followup_patients

    , htn_screened_patients - lost_followup_patients AS med_received_patients

FROM 
    {{ ref("stg_agg_htn_info_monthly") }}