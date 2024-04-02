{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "dump"]
    )
-}}

-- Report 9D
SELECT
    mdst.patient_id AS `Patient_ID`,
    mdst.collected_date AS `Collection_Date`,
    pe.registration_id AS `Registration_ID`,
    pe.given_name AS `Given_Name`,
    pe.family_name AS `Family_Name`,
    pe.gender_code AS `Gender`,
    pe.birth_date AS `Birth_Date`,
    pe.age AS `Age`,
    pe.cell_number AS `Mobile_Number`,
    mdst.height AS `Height`,
    mdst.weight AS `Weight`,
    mdst.bmi AS `BMI`,
    mdst.bmi_status AS `BMI_Status`,
    mdst.muac AS `MUAC`,
    mdst.muac_status AS `MUAC_Status`,
    pe.gender_description AS `Gender_Description`,
    mdst.heart_rate AS `Heart_Rate`,
    mdst.bp_systolic_1 AS `BP_Systolic_1`,
    mdst.bp_diastolic_1 AS `BP_Diastolic_1`,
    mdst.bp_systolic_2 AS `BP_Systolic_2`,
    mdst.bp_diastolic_2 AS `BP_Diastolic_2`,
    mdst.bp_collected_date AS `BP_Collection_Date`,
    mdst.rbg AS `RBG`,
    mdst.fbg AS `FBG`,
    mdst.hrs_from_last_eat AS `Hours_Since_Last_Meal`,
    mdst.hemoglobin AS `Hemoglobin`,
    mdst.anemia_severity AS `Anemia_Severity`,
    mdst.cough_greater_than_month AS `Cough_Greater_Than_Month`,
    mdst.lgerf AS `LGERF`,
    mdst.night_sweat AS `Night_Sweat`,
    mdst.weight_loss AS `Weight_Loss`,
    IF(mdst.is_pregnant, 1, 0) AS `Is_Pregnant`,
    mdst.gravida AS `Gravida`,
    mdst.still_birth AS `Still_Birth`,
    mdst.miscarraige_or_abortion AS `Miscarraige_or_Abortion`,
    mdst.mr AS `MR`,
    mdst.living_birth AS `Living_Birth`,
    mdst.living_male AS `Living_Male`,
    mdst.living_female AS `Living_Female`,
    mdst.child_mortality_0_to_1 AS `Child_Mortality_0_to_1`,
    mdst.child_mortality_below_5 AS `Child_Mortality_Below_5`,
    mdst.child_mortality_over_5 AS `Child_Mortality_Over_5`,
    mdst.lmp AS `LMP`,
    mdst.contraception_method_code AS `Contraception_Method_Code`,
    mdst.other_contraception_method AS `Other_Contraception_Method`,
    mdst.menstruation_product_code AS `Menstruation_Product_Code`,
    mdst.menstruation_product_usage_time_code AS `Menstruation_Product_Usage_Time`,
    mdst.other_menstruation_product AS `Other_Menstruation_Product`,
    mdst.other_menstruation_product_usage_time AS `Other_Menstruation_Product_Usage_Time`,
    mdst.prescribed_drugs AS `Prescribed_Drugs`,
    mdst.chief_complain_with_duration AS `Chief_Complain_with_Duration`,
    mdst.rx_details AS `RX_Details`,
    mdst.provisional_diagnosis_details AS `Provisional_Diagnosis`,
    mdst.physical_findings AS `Physical_Findings`,
    mdst.patient_illness_history AS `Patient_Illness_History`,
    mdst.family_illness_history AS `Family_Illness_History`,
    mdst.social_behavior_history AS `Social_Behavior_History`,
    mdst.vaccination AS `Vaccination`

FROM
    {{ ref("bse_dbo_prescription_creation") }} AS pc

LEFT OUTER JOIN {{ ref("stg_cor_mdata_super_table") }} AS mdst
    ON
        pc.patient_id = mdst.patient_id
        AND pc.created_date = mdst.collected_date

LEFT OUTER JOIN {{ ref("stg_cor_patient_extended") }} AS pe
    ON
        mdst.patient_id = pe.patient_id
