{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "super_table"]
    )
-}}

SELECT
    patient_id,
    collected_date,

    -- mdatapatientobsgynae information
    COALESCE(mdobs.is_pregnant, false) AS is_pregnant,
    mdobs.gravida,
    mdobs.still_birth,
    mdobs.miscarraige_or_abortion,
    mdobs.mr,
    mdobs.living_birth,
    mdobs.living_male,
    mdobs.living_female,
    mdobs.child_mortality_0_to_1,
    mdobs.child_mortality_below_5,
    mdobs.child_mortality_over_5,
    mdobs.lmp,
    mdobs.other_contraception_method,
    mdobs.other_menstruation_product,
    mdobs.other_menstruation_product_usage_time,
    mdobs.contraception_method_code,
    mdobs.menstruation_product_code,
    mdobs.menstruation_product_usage_time_code,

    -- mdatabp information 
    mdbp.collected_date AS bp_collected_date,
    mdbp.heart_rate,
    mdbp.bp_systolic_1,
    mdbp.bp_systolic_2,
    mdbp.bp_diastolic_1,
    mdbp.bp_diastolic_2,
    COALESCE(mdbp.bp_systolic_2, mdbp.bp_systolic_1) AS bp_systolic,
    COALESCE(mdbp.bp_diastolic_2, mdbp.bp_diastolic_1) AS bp_diastolic,

    -- mdataglucosehb information
    mdghb.fbg,
    mdghb.rbg,
    mdghb.hrs_from_last_eat,
    mdghb.hemoglobin,

    -- mdatafolloqupdate information
    mdfd.followup_date_id,
    mdfd.followup_date,
    LEAD(collected_date, 1, "3000-01-01") OVER (PARTITION BY patient_id ORDER BY collected_date ASC) AS next_collected_date,

    -- mdataheightweight information 
    mdhw.height,
    mdhw.weight,
    mdhw.bmi,
    mdhw.bmi_status,
    mdhw.muac,
    mdhw.muac_status,

    -- mdatavarioussymptom information 
    -- mdvs.anemia_severity,
    mdvs.cough_greater_than_month,
    mdvs.lgerf,
    mdvs.night_sweat,
    mdvs.weight_loss,

    -- mdataphysicalexamgeneral
    mdpeg.anemia_severity,
    mdpeg.edema_severity,
    mdpeg.jaundice_severity,
    mdpeg.is_heart_with_nad,
    mdpeg.is_lungs_with_nad,
    mdpeg.is_lymph_nodes_with_palpable,
    mdpeg.heart_with_nad,
    mdpeg.lungs_with_nad,
    mdpeg.lymph_nodes_with_palpable_site,
    mdpeg.lymph_nodes_with_palpable_size,

    -- mdatatreatmentsuggestion information
    mdtse.prescribed_drugs,

    -- mdatapatientreferral information
    mdpre.referred_to_health_center_id,
    mdpre.referred_to_health_center_name,

    -- mdataphysicalfinding information
    mdpfe.physical_findings,

    -- mdatasocialbehavior information
    mdsbe.social_behavior_history,

    -- mdatapatientccdetails information
    mdpccde.chief_complain_with_duration,

    -- mdatapatientvaccine information
    COALESCE(mdpve.is_vac_bcg, false) AS is_vac_bcg,
    COALESCE(mdpve.is_vac_pentavalent, false) AS is_vac_pentavalent,
    COALESCE(mdpve.is_vac_pcv, false) AS is_vac_pcv,
    COALESCE(mdpve.is_vac_opv, false) AS is_vac_opv,
    COALESCE(mdpve.is_vac_mrs, false) AS is_vac_mrs,
    COALESCE(mdpve.is_vac_measles, false) AS is_vac_measles,
    COALESCE(mdpve.is_vac_chicken_pox, false) AS is_vac_chicken_pox,
    COALESCE(mdpve.is_vac_covid_19, false) AS is_vac_covid_19,
    mdpve.vaccination,

    -- mdatarxdetails information
    mdrxde.rx_details,

    -- mdataprovisionaldiagnosis information
    mdpde.provisional_diagnosis_details,

    -- mdatapatientillnesshistory information
    mdpihe.patient_illness_history,
    COALESCE(mdpihe.is_asthma, false) AS is_asthma,
    COALESCE(mdpihe.is_cancer, false) AS is_cancer,
    COALESCE(mdpihe.is_dengue, false) AS is_dengue,
    COALESCE(mdpihe.is_depression, false) AS is_depression,
    COALESCE(mdpihe.is_dm, false) AS is_dm,
    COALESCE(mdpihe.is_fracture_injury, false) AS is_fracture_injury,
    COALESCE(mdpihe.is_hepatitis, false) AS is_hepatitis,
    COALESCE(mdpihe.is_hypertension, false) AS is_hypertension,
    COALESCE(mdpihe.is_ihd, false) AS is_ihd,
    COALESCE(mdpihe.is_malaria, false) AS is_malaria,
    COALESCE(mdpihe.is_others, false) AS is_others,
    COALESCE(mdpihe.is_skin_disease, false) AS is_skin_disease,
    COALESCE(mdpihe.is_stroke, false) AS is_stroke,
    COALESCE(mdpihe.is_surgery, false) AS is_surgery,
    COALESCE(mdpihe.is_tb, false) AS is_tb,
    COALESCE(mdpihe.is_typhoid, false) AS is_typhoid,

    -- mdatafamilyillnesshistory information
    COALESCE(mdfihe.is_aunt_asthma, false) AS is_aunt_asthma,
    COALESCE(mdfihe.is_aunt_cancer, false) AS is_aunt_cancer,
    COALESCE(mdfihe.is_aunt_dengue, false) AS is_aunt_dengue,
    COALESCE(mdfihe.is_aunt_depression, false) AS is_aunt_depression,
    COALESCE(mdfihe.is_aunt_dm, false) AS is_aunt_dm,
    COALESCE(mdfihe.is_aunt_fracture_injury, false) AS is_aunt_fracture_injury,
    COALESCE(mdfihe.is_aunt_hepatitis, false) AS is_aunt_hepatitis,
    COALESCE(mdfihe.is_aunt_hypertension, false) AS is_aunt_hypertension,
    COALESCE(mdfihe.is_aunt_ihd, false) AS is_aunt_ihd,
    COALESCE(mdfihe.is_aunt_malaria, false) AS is_aunt_malaria,
    COALESCE(mdfihe.is_aunt_others, false) AS is_aunt_others,
    COALESCE(mdfihe.is_aunt_skin_disease, false) AS is_aunt_skin_disease,
    COALESCE(mdfihe.is_aunt_stroke, false) AS is_aunt_stroke,
    COALESCE(mdfihe.is_aunt_surgery, false) AS is_aunt_surgery,
    COALESCE(mdfihe.is_aunt_tb, false) AS is_aunt_tb,
    COALESCE(mdfihe.is_aunt_typhoid, false) AS is_aunt_typhoid,
    COALESCE(mdfihe.is_father_asthma, false) AS is_father_asthma,
    COALESCE(mdfihe.is_father_cancer, false) AS is_father_cancer,
    COALESCE(mdfihe.is_father_dengue, false) AS is_father_dengue,
    COALESCE(mdfihe.is_father_depression, false) AS is_father_depression,
    COALESCE(mdfihe.is_father_dm, false) AS is_father_dm,
    COALESCE(mdfihe.is_father_fracture_injury, false) AS is_father_fracture_injury,
    COALESCE(mdfihe.is_father_hepatitis, false) AS is_father_hepatitis,
    COALESCE(mdfihe.is_father_hypertension, false) AS is_father_hypertension,
    COALESCE(mdfihe.is_father_ihd, false) AS is_father_ihd,
    COALESCE(mdfihe.is_father_malaria, false) AS is_father_malaria,
    COALESCE(mdfihe.is_father_others, false) AS is_father_others,
    COALESCE(mdfihe.is_father_skin_disease, false) AS is_father_skin_disease,
    COALESCE(mdfihe.is_father_stroke, false) AS is_father_stroke,
    COALESCE(mdfihe.is_father_surgery, false) AS is_father_surgery,
    COALESCE(mdfihe.is_father_tb, false) AS is_father_tb,
    COALESCE(mdfihe.is_father_typhoid, false) AS is_father_typhoid,
    COALESCE(mdfihe.is_grandparents_asthma, false) AS is_grandparents_asthma,
    COALESCE(mdfihe.is_grandparents_cancer, false) AS is_grandparents_cancer,
    COALESCE(mdfihe.is_grandparents_dengue, false) AS is_grandparents_dengue,
    COALESCE(mdfihe.is_grandparents_depression, false) AS is_grandparents_depression,
    COALESCE(mdfihe.is_grandparents_dm, false) AS is_grandparents_dm,
    COALESCE(mdfihe.is_grandparents_fracture_injury, false) AS is_grandparents_fracture_injury,
    COALESCE(mdfihe.is_grandparents_hepatitis, false) AS is_grandparents_hepatitis,
    COALESCE(mdfihe.is_grandparents_hypertension, false) AS is_grandparents_hypertension,
    COALESCE(mdfihe.is_grandparents_ihd, false) AS is_grandparents_ihd,
    COALESCE(mdfihe.is_grandparents_malaria, false) AS is_grandparents_malaria,
    COALESCE(mdfihe.is_grandparents_others, false) AS is_grandparents_others,
    COALESCE(mdfihe.is_grandparents_skin_disease, false) AS is_grandparents_skin_disease,
    COALESCE(mdfihe.is_grandparents_stroke, false) AS is_grandparents_stroke,
    COALESCE(mdfihe.is_grandparents_surgery, false) AS is_grandparents_surgery,
    COALESCE(mdfihe.is_grandparents_tb, false) AS is_grandparents_tb,
    COALESCE(mdfihe.is_grandparents_typhoid, false) AS is_grandparents_typhoid,
    COALESCE(mdfihe.is_mother_asthma, false) AS is_mother_asthma,
    COALESCE(mdfihe.is_mother_cancer, false) AS is_mother_cancer,
    COALESCE(mdfihe.is_mother_dengue, false) AS is_mother_dengue,
    COALESCE(mdfihe.is_mother_depression, false) AS is_mother_depression,
    COALESCE(mdfihe.is_mother_dm, false) AS is_mother_dm,
    COALESCE(mdfihe.is_mother_fracture_injury, false) AS is_mother_fracture_injury,
    COALESCE(mdfihe.is_mother_hepatitis, false) AS is_mother_hepatitis,
    COALESCE(mdfihe.is_mother_hypertension, false) AS is_mother_hypertension,
    COALESCE(mdfihe.is_mother_ihd, false) AS is_mother_ihd,
    COALESCE(mdfihe.is_mother_malaria, false) AS is_mother_malaria,
    COALESCE(mdfihe.is_mother_others, false) AS is_mother_others,
    COALESCE(mdfihe.is_mother_skin_disease, false) AS is_mother_skin_disease,
    COALESCE(mdfihe.is_mother_stroke, false) AS is_mother_stroke,
    COALESCE(mdfihe.is_mother_surgery, false) AS is_mother_surgery,
    COALESCE(mdfihe.is_mother_tb, false) AS is_mother_tb,
    COALESCE(mdfihe.is_mother_typhoid, false) AS is_mother_typhoid,
    COALESCE(mdfihe.is_other_asthma, false) AS is_other_asthma,
    COALESCE(mdfihe.is_other_cancer, false) AS is_other_cancer,
    COALESCE(mdfihe.is_other_dengue, false) AS is_other_dengue,
    COALESCE(mdfihe.is_other_depression, false) AS is_other_depression,
    COALESCE(mdfihe.is_other_dm, false) AS is_other_dm,
    COALESCE(mdfihe.is_other_fracture_injury, false) AS is_other_fracture_injury,
    COALESCE(mdfihe.is_other_hepatitis, false) AS is_other_hepatitis,
    COALESCE(mdfihe.is_other_hypertension, false) AS is_other_hypertension,
    COALESCE(mdfihe.is_other_ihd, false) AS is_other_ihd,
    COALESCE(mdfihe.is_other_malaria, false) AS is_other_malaria,
    COALESCE(mdfihe.is_other_others, false) AS is_other_others,
    COALESCE(mdfihe.is_other_skin_disease, false) AS is_other_skin_disease,
    COALESCE(mdfihe.is_other_stroke, false) AS is_other_stroke,
    COALESCE(mdfihe.is_other_surgery, false) AS is_other_surgery,
    COALESCE(mdfihe.is_other_tb, false) AS is_other_tb,
    COALESCE(mdfihe.is_other_typhoid, false) AS is_other_typhoid,
    COALESCE(mdfihe.is_siblings_asthma, false) AS is_siblings_asthma,
    COALESCE(mdfihe.is_siblings_cancer, false) AS is_siblings_cancer,
    COALESCE(mdfihe.is_siblings_dengue, false) AS is_siblings_dengue,
    COALESCE(mdfihe.is_siblings_depression, false) AS is_siblings_depression,
    COALESCE(mdfihe.is_siblings_dm, false) AS is_siblings_dm,
    COALESCE(mdfihe.is_siblings_fracture_injury, false) AS is_siblings_fracture_injury,
    COALESCE(mdfihe.is_siblings_hepatitis, false) AS is_siblings_hepatitis,
    COALESCE(mdfihe.is_siblings_hypertension, false) AS is_siblings_hypertension,
    COALESCE(mdfihe.is_siblings_ihd, false) AS is_siblings_ihd,
    COALESCE(mdfihe.is_siblings_malaria, false) AS is_siblings_malaria,
    COALESCE(mdfihe.is_siblings_others, false) AS is_siblings_others,
    COALESCE(mdfihe.is_siblings_skin_disease, false) AS is_siblings_skin_disease,
    COALESCE(mdfihe.is_siblings_stroke, false) AS is_siblings_stroke,
    COALESCE(mdfihe.is_siblings_surgery, false) AS is_siblings_surgery,
    COALESCE(mdfihe.is_siblings_tb, false) AS is_siblings_tb,
    COALESCE(mdfihe.is_siblings_typhoid, false) AS is_siblings_typhoid,
    mdfihe.family_illness_history

FROM
    {{ ref("bse_dbo_mdata_bp") }} AS mdbp

FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_obs_gynae_extended") }} AS mdobs
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("bse_dbo_mdata_glucose_hb") }} AS mdghb
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("bse_dbo_mdata_followup_date") }} AS mdfd
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("bse_dbo_mdata_height_weight") }} AS mdhw
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("bse_dbo_mdata_various_symptom") }} AS mdvs
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("bse_dbo_mdata_physical_exam_general") }} AS mdpeg
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_treatment_suggestion_extended") }} AS mdtse
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_referral_extended") }} AS mdpre
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_physical_finding_extended") }} AS mdpfe
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_social_behavior_extended") }} AS mdsbe
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_cc_details_extended") }} AS mdpccde
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_vaccine_extended") }} AS mdpve
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_rx_details_extended") }} AS mdrxde
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_provisional_diagnosis_extended") }} AS mdpde
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_illness_history_pivoted") }} AS mdpihe
    USING (patient_id, collected_date)

FULL OUTER JOIN {{ ref("stg_cor_mdata_family_illness_history_pivoted") }} AS mdfihe
    USING (patient_id, collected_date)
