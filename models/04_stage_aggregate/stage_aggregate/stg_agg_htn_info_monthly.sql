{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "agg", "htn"]
    )
-}}

WITH registered_patients AS (
    SELECT 
        DATE(DATE_TRUNC(created_at, MONTH)) AS period_start_date
        , workplace_id
        , workplace_name
        , workplace_branch_code
        , district_name 
        , COUNT(patient_id) AS registered_patients

    FROM 
        {{ ref("stg_cor_patient_extended") }}

    GROUP BY 
        period_start_date 
        , workplace_id
        , workplace_name
        , workplace_branch_code
        , district_name 
) 

, screened_patients AS (
    SELECT
        DATE(DATE_TRUNC(collected_date, MONTH)) AS period_start_date
        , p.workplace_id
        , p.workplace_name 
        , p.workplace_branch_code
        , p.district_name
        , COUNT(CASE WHEN bp_systolic > 140 OR bp_diastolic > 90 THEN mdata.patient_id END) AS htn_screened_patients
        , COUNT(CASE WHEN bp_systolic <= 140 AND bp_diastolic <= 90 THEN mdata.patient_id END) AS non_htn_patients
        , COUNT(CASE WHEN (bp_systolic > 140 OR bp_diastolic > 90) AND followup_date IS NOT NULL AND DATE_DIFF(next_collected_date, followup_date, DAY) > 14 THEN mdata.patient_id END) AS lost_followup_patients

    FROM
        {{ ref("stg_cor_mdata_super_table") }} AS mdata

        LEFT JOIN {{ ref("stg_cor_patient_extended") }} AS p
            ON
                mdata.patient_id = p.patient_id

    WHERE 
        TRUE
        AND bp_systolic IS NOT NULL
    
    GROUP BY 
        period_start_date 
        , workplace_id
        , workplace_name
        , workplace_branch_code
        , district_name 
)

SELECT 
    rp.period_start_date
    , rp.workplace_id
    , rp.workplace_name
    , rp.workplace_branch_code
    , rp.district_name 
    , IFNULL(rp.registered_patients, 0) AS registered_patients
    , IFNULL(sp.htn_screened_patients, 0) AS htn_screened_patients
    , IFNULL(sp.non_htn_patients, 0) AS non_htn_patients
    , IFNULL(sp.lost_followup_patients, 0) AS lost_followup_patients
    , SUM(IFNULL(rp.registered_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_registered_patients
    , SUM(IFNULL(sp.htn_screened_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_htn_screened_patients
    , SUM(IFNULL(sp.non_htn_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_non_htn_patients
    , SUM(IFNULL(sp.lost_followup_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_lost_followup_patients

FROM 
    registered_patients AS rp 
    
    LEFT JOIN screened_patients AS sp 
    ON 
        rp.period_start_date = sp.period_start_date
        AND rp.workplace_id = sp.workplace_id
        AND rp.workplace_name = sp.workplace_name
        AND rp.district_name = sp.district_name

WINDOW
    previous_all_days_cumulative AS (
        PARTITION BY rp.workplace_id, rp.workplace_name, rp.workplace_branch_code, rp.district_name  
        ORDER BY rp.period_start_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )