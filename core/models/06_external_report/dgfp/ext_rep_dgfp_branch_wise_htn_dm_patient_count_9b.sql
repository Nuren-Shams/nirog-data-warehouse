{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "external", "report", "htn_dm", "branch_wise"]
    )
-}}

SELECT
    `District_Name`,
    `Upazila_Name`,
    `Union_Name`,
    `Health_Center_Name`,
    `Barcode_Prefix`,
    `Period_Start_Date`,
    `Registered_Patients`,
    `HTN_Screened_Patients`,
    `HTN_Screened_Male_Patients`,
    `HTN_Screened_Female_Patients`,
    `HTN_Diagnosed_Patients`,
    `DM_Screened_Patients`,
    `DM_Screened_Male_Patients`,
    `DM_Screened_Female_Patients`,
    `DM_Diagnosed_Patients`

FROM
    {{ ref('rep_branch_wise_htn_dm_patient_count_9b') }}

WHERE
    TRUE
    {{ mcr_dgfp_health_center_filter("`Health_Center_Name`") }}
