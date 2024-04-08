{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn_dm", "branch_wise"]
    )
-}}

SELECT
    district_name AS `District_Name`,
    upazila_name AS `Upazila_Name`,
    union_name AS `Union_Name`,
    health_center_name AS `Health_Center_Name`,
    barcode_prefix AS `Barcode_Prefix`,
    period_start_date AS `Period_Start_Date`,
    registered_patients AS `Registered_Patients`,
    h.htn_screened_patients AS `HTN_Screened_Patients`,
    h.htn_screened_male_patients AS `HTN_Screened_Male_Patients`,
    h.htn_screened_female_patients AS `HTN_Screened_Female_Patients`,
    h.htn_diagnosed_patients AS `HTN_Diagnosed_Patients`,
    d.dm_screened_patients AS `DM_Screened_Patients`,
    d.dm_screened_male_patients AS `DM_Screened_Male_Patients`,
    d.dm_screened_female_patients AS `DM_Screened_Female_Patients`,
    d.dm_diagnosed_patients AS `DM_Diagnosed_Patients`
FROM
    {{ ref('stg_agg_htn_info_daily') }} AS h
FULL OUTER JOIN
    {{ ref('stg_agg_dm_info_daily') }} AS d
    USING (district_name, upazila_name, union_name, health_center_name, barcode_prefix, registered_patients, period_start_date)
    -- ON
    --     h.district_name = d.district_name
    --     AND h.upazila_name = d.upazila_name
    --     AND h.union_name = d.union_name
    --     AND h.health_center_name = d.health_center_name
    --     AND h.barcode_prefix = d.barcode_prefix
    --     AND h.total_registered_patients = d.total_registered_patients
ORDER BY
    district_name,
    upazila_name,
    union_name,
    health_center_name
