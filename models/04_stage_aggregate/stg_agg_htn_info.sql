{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "agg", "htn"]
    )
-}}

SELECT
    district_name
    , upazila_name
    , union_name
    , health_center_name
    , barcode_prefix
    , SUM(registered_patients) AS total_registered_patients
    , SUM(htn_diagnosed_patients) AS total_htn_diagnosed_patients
    , SUM(non_htn_patients) AS total_non_htn_patients
    , SUM(medication_received_patients) AS total_medication_received_patients
    , SUM(lost_followup_patients) AS total_lost_followup_patients

FROM 
    {{ ref('stg_agg_htn_info_daily') }}
    
GROUP BY 
    district_name
    , upazila_name
    , union_name
    , health_center_name
    , barcode_prefix
