{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "patient details"]
    )
-}}

-- Report 9a
SELECT
    mdst.collected_date AS `Collection_Date`,
    mdst.patient_id AS `Patient_ID`,
    pe.registration_id AS `Registration_ID`,
    pe.health_center_name AS `Health_Center_Name`,
    pe.district_name AS `District_Name`,
    pe.upazila_name AS `Upazila_Name`,
    pe.union_name AS `Union_Name`,
    CONCAT(pe.given_name, " ", pe.family_name) AS `Patient_Name`,
    pe.gender_code AS `Gender`,
    pe.age AS `Age`,
    pe.spouse_name AS `Spouse`,
    pe.father_name AS `Father`,
    pe.mother_name AS `Mother`,
    pe.id_type AS `ID_Type`,
    pe.id_number AS `ID_Number`,
    pe.cell_number AS `Mobile_Number`,
    "" AS `Address`,
    mdst.bp_systolic AS `BP_Systolic`,
    mdst.bp_diastolic AS `BP_Diastolic`,
    mdst.rbg AS `RBG`,
    mdst.fbg AS `FBG`,
    mdst.chief_complain_with_duration AS `Chief_Complain_with_Duration`,
    mdst.provisional_diagnosis_details AS `Provisional_Diagnosis`,
    mdst.prescribed_drugs AS `Prescribed_Drugs`,
    mdst.followup_date AS `Followup_Date`,
    mdst.referred_to_health_center_name AS `Referred_to`,
    CASE WHEN
        LAG(pc.created_date, 1) OVER (PARTITION BY pc.patient_id ORDER BY pc.created_date ASC) IS NULL THEN "NEW"
    ELSE "REVISIT" END
        AS `Patient_Type`

FROM
    {{ ref("bse_dbo_prescription_creation") }} AS pc

LEFT OUTER JOIN {{ ref("stg_cor_mdata_super_table") }} AS mdst
    ON
        pc.patient_id = mdst.patient_id
        AND pc.created_date = mdst.collected_date

LEFT OUTER JOIN {{ ref("stg_cor_patient_extended") }} AS pe
    ON
        mdst.patient_id = pe.patient_id

WHERE
    TRUE
    AND pe.registration_id IS NOT NULL
