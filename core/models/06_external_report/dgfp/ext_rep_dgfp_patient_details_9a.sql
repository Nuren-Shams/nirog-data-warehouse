{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "external", "report", "patient details"]
    )
-}}

-- Report 9a
SELECT
    `Collection_Date`,
    `Patient_ID`,
    `Registration_ID`,
    `Health_Center_Name`,
    `District_Name`,
    `Upazila_Name`,
    `Union_Name`,
    `Patient_Name`,
    `Gender`,
    `Age`,
    `Spouse`,
    `Father`,
    `Mother`,
    `ID_Type`,
    `ID_Number`,
    `Mobile_Number`,
    `Address`,
    `BP_Systolic`,
    `BP_Diastolic`,
    `RBG`,
    `FBG`,
    `Chief_Complain_with_Duration`,
    `Provisional_Diagnosis`,
    `Prescribed_Drugs`,
    `Followup_Date`,
    `Referred_to`,
    `Patient_Type`

FROM
    {{ ref("rep_patient_details_9a") }}

WHERE TRUE
    {{ mcr_dgfp_health_center_filter("`Health_Center_Name`") }}
