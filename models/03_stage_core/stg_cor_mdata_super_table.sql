{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "super_table"]
    )
-}}

SELECT 
    patient_id
    , collected_date
    , IFNULL(mdobs.is_pregnant, false) AS is_pregnant
    , mdbp.bp_systolic_1
    , mdbp.bp_systolic_2
    , mdbp.bp_diastolic_1
    , mdbp.bp_diastolic_2
    , COALESCE(mdbp.bp_systolic_2, mdbp.bp_systolic_1) AS bp_systolic 
    , COALESCE(mdbp.bp_diastolic_2, mdbp.bp_diastolic_1) AS bp_diastolic
    , mdghb.fbg
    , mdghb.rbg
    , mdfd.followup_date_id
    , mdfd.followup_date
    , LEAD(collected_date, 1, "3000-01-01") OVER(PARTITION BY patient_id ORDER BY collected_date ASC) AS next_collected_date
    , mdhw.height
    , mdhw.weight
    , mdhw.bmi
    , mdtse.prescribed_drugs
    , mdpccde.chief_complain_with_duration
    , COALESCE(mdpve.is_vac_bcg, FALSE) AS is_vac_bcg
    , COALESCE(mdpve.is_vac_pentavalent, FALSE) AS is_vac_pentavalent 
    , COALESCE(mdpve.is_vac_pcv, FALSE) AS is_vac_pcv
    , COALESCE(mdpve.is_vac_opv, FALSE) AS is_vac_opv
    , COALESCE(mdpve.is_vac_mrs, FALSE) AS is_vac_mrs
    , COALESCE(mdpve.is_vac_measles, FALSE) AS is_vac_measles
    , COALESCE(mdpve.is_vac_chicken_pox, FALSE) AS is_vac_chicken_pox
    , COALESCE(mdpve.is_vac_covid_19, FALSE) AS is_vac_covid_19
    , mdrxde.rx_details
    , mdpde.provisional_diagnosis_details 
    , COALESCE(mdfihe.is_aunt_asthma, FALSE) AS is_aunt_asthma
    , COALESCE(mdfihe.is_aunt_cancer, FALSE) AS is_aunt_cancer
    , COALESCE(mdfihe.is_aunt_dengue, FALSE) AS is_aunt_dengue
    , COALESCE(mdfihe.is_aunt_depression, FALSE) AS is_aunt_depression
    , COALESCE(mdfihe.is_aunt_dm, FALSE) AS is_aunt_dm
    , COALESCE(mdfihe.is_aunt_fracture_injury, FALSE) AS is_aunt_fracture_injury
    , COALESCE(mdfihe.is_aunt_hepatitis, FALSE) AS is_aunt_hepatitis
    , COALESCE(mdfihe.is_aunt_hypertension, FALSE) AS is_aunt_hypertension
    , COALESCE(mdfihe.is_aunt_ihd, FALSE) AS is_aunt_ihd
    , COALESCE(mdfihe.is_aunt_malaria, FALSE) AS is_aunt_malaria
    , COALESCE(mdfihe.is_aunt_others, FALSE) AS is_aunt_others
    , COALESCE(mdfihe.is_aunt_skin_disease, FALSE) AS is_aunt_skin_disease
    , COALESCE(mdfihe.is_aunt_stroke, FALSE) AS is_aunt_stroke
    , COALESCE(mdfihe.is_aunt_surgery, FALSE) AS is_aunt_surgery
    , COALESCE(mdfihe.is_aunt_tb, FALSE) AS is_aunt_tb
    , COALESCE(mdfihe.is_aunt_typhoid, FALSE) AS is_aunt_typhoid
    , COALESCE(mdfihe.is_father_asthma, FALSE) AS is_father_asthma
    , COALESCE(mdfihe.is_father_cancer, FALSE) AS is_father_cancer
    , COALESCE(mdfihe.is_father_dengue, FALSE) AS is_father_dengue
    , COALESCE(mdfihe.is_father_depression, FALSE) AS is_father_depression
    , COALESCE(mdfihe.is_father_dm, FALSE) AS is_father_dm
    , COALESCE(mdfihe.is_father_fracture_injury, FALSE) AS is_father_fracture_injury
    , COALESCE(mdfihe.is_father_hepatitis, FALSE) AS is_father_hepatitis
    , COALESCE(mdfihe.is_father_hypertension, FALSE) AS is_father_hypertension
    , COALESCE(mdfihe.is_father_ihd, FALSE) AS is_father_ihd
    , COALESCE(mdfihe.is_father_malaria, FALSE) AS is_father_malaria
    , COALESCE(mdfihe.is_father_others, FALSE) AS is_father_others
    , COALESCE(mdfihe.is_father_skin_disease, FALSE) AS is_father_skin_disease
    , COALESCE(mdfihe.is_father_stroke, FALSE) AS is_father_stroke
    , COALESCE(mdfihe.is_father_surgery, FALSE) AS is_father_surgery
    , COALESCE(mdfihe.is_father_tb, FALSE) AS is_father_tb
    , COALESCE(mdfihe.is_father_typhoid, FALSE) AS is_father_typhoid
    , COALESCE(mdfihe.is_grandparents_asthma, FALSE) AS is_grandparents_asthma
    , COALESCE(mdfihe.is_grandparents_cancer, FALSE) AS is_grandparents_cancer
    , COALESCE(mdfihe.is_grandparents_dengue, FALSE) AS is_grandparents_dengue
    , COALESCE(mdfihe.is_grandparents_depression, FALSE) AS is_grandparents_depression
    , COALESCE(mdfihe.is_grandparents_dm, FALSE) AS is_grandparents_dm
    , COALESCE(mdfihe.is_grandparents_fracture_injury, FALSE) AS is_grandparents_fracture_injury
    , COALESCE(mdfihe.is_grandparents_hepatitis, FALSE) AS is_grandparents_hepatitis
    , COALESCE(mdfihe.is_grandparents_hypertension, FALSE) AS is_grandparents_hypertension
    , COALESCE(mdfihe.is_grandparents_ihd, FALSE) AS is_grandparents_ihd
    , COALESCE(mdfihe.is_grandparents_malaria, FALSE) AS is_grandparents_malaria
    , COALESCE(mdfihe.is_grandparents_others, FALSE) AS is_grandparents_others
    , COALESCE(mdfihe.is_grandparents_skin_disease, FALSE) AS is_grandparents_skin_disease
    , COALESCE(mdfihe.is_grandparents_stroke, FALSE) AS is_grandparents_stroke
    , COALESCE(mdfihe.is_grandparents_surgery, FALSE) AS is_grandparents_surgery
    , COALESCE(mdfihe.is_grandparents_tb, FALSE) AS is_grandparents_tb
    , COALESCE(mdfihe.is_grandparents_typhoid, FALSE) AS is_grandparents_typhoid
    , COALESCE(mdfihe.is_mother_asthma, FALSE) AS is_mother_asthma
    , COALESCE(mdfihe.is_mother_cancer, FALSE) AS is_mother_cancer
    , COALESCE(mdfihe.is_mother_dengue, FALSE) AS is_mother_dengue
    , COALESCE(mdfihe.is_mother_depression, FALSE) AS is_mother_depression
    , COALESCE(mdfihe.is_mother_dm, FALSE) AS is_mother_dm
    , COALESCE(mdfihe.is_mother_fracture_injury, FALSE) AS is_mother_fracture_injury
    , COALESCE(mdfihe.is_mother_hepatitis, FALSE) AS is_mother_hepatitis
    , COALESCE(mdfihe.is_mother_hypertension, FALSE) AS is_mother_hypertension
    , COALESCE(mdfihe.is_mother_ihd, FALSE) AS is_mother_ihd
    , COALESCE(mdfihe.is_mother_malaria, FALSE) AS is_mother_malaria
    , COALESCE(mdfihe.is_mother_others, FALSE) AS is_mother_others
    , COALESCE(mdfihe.is_mother_skin_disease, FALSE) AS is_mother_skin_disease
    , COALESCE(mdfihe.is_mother_stroke, FALSE) AS is_mother_stroke
    , COALESCE(mdfihe.is_mother_surgery, FALSE) AS is_mother_surgery
    , COALESCE(mdfihe.is_mother_tb, FALSE) AS is_mother_tb
    , COALESCE(mdfihe.is_mother_typhoid, FALSE) AS is_mother_typhoid
    , COALESCE(mdfihe.is_other_asthma, FALSE) AS is_other_asthma
    , COALESCE(mdfihe.is_other_cancer, FALSE) AS is_other_cancer
    , COALESCE(mdfihe.is_other_dengue, FALSE) AS is_other_dengue
    , COALESCE(mdfihe.is_other_depression, FALSE) AS is_other_depression
    , COALESCE(mdfihe.is_other_dm, FALSE) AS is_other_dm
    , COALESCE(mdfihe.is_other_fracture_injury, FALSE) AS is_other_fracture_injury
    , COALESCE(mdfihe.is_other_hepatitis, FALSE) AS is_other_hepatitis
    , COALESCE(mdfihe.is_other_hypertension, FALSE) AS is_other_hypertension
    , COALESCE(mdfihe.is_other_ihd, FALSE) AS is_other_ihd
    , COALESCE(mdfihe.is_other_malaria, FALSE) AS is_other_malaria
    , COALESCE(mdfihe.is_other_others, FALSE) AS is_other_others
    , COALESCE(mdfihe.is_other_skin_disease, FALSE) AS is_other_skin_disease
    , COALESCE(mdfihe.is_other_stroke, FALSE) AS is_other_stroke
    , COALESCE(mdfihe.is_other_surgery, FALSE) AS is_other_surgery
    , COALESCE(mdfihe.is_other_tb, FALSE) AS is_other_tb
    , COALESCE(mdfihe.is_other_typhoid, FALSE) AS is_other_typhoid
    , COALESCE(mdfihe.is_siblings_asthma, FALSE) AS is_siblings_asthma
    , COALESCE(mdfihe.is_siblings_cancer, FALSE) AS is_siblings_cancer
    , COALESCE(mdfihe.is_siblings_dengue, FALSE) AS is_siblings_dengue
    , COALESCE(mdfihe.is_siblings_depression, FALSE) AS is_siblings_depression
    , COALESCE(mdfihe.is_siblings_dm, FALSE) AS is_siblings_dm
    , COALESCE(mdfihe.is_siblings_fracture_injury, FALSE) AS is_siblings_fracture_injury
    , COALESCE(mdfihe.is_siblings_hepatitis, FALSE) AS is_siblings_hepatitis
    , COALESCE(mdfihe.is_siblings_hypertension, FALSE) AS is_siblings_hypertension
    , COALESCE(mdfihe.is_siblings_ihd, FALSE) AS is_siblings_ihd
    , COALESCE(mdfihe.is_siblings_malaria, FALSE) AS is_siblings_malaria
    , COALESCE(mdfihe.is_siblings_others, FALSE) AS is_siblings_others
    , COALESCE(mdfihe.is_siblings_skin_disease, FALSE) AS is_siblings_skin_disease
    , COALESCE(mdfihe.is_siblings_stroke, FALSE) AS is_siblings_stroke
    , COALESCE(mdfihe.is_siblings_surgery, FALSE) AS is_siblings_surgery
    , COALESCE(mdfihe.is_siblings_tb, FALSE) AS is_siblings_tb
    , COALESCE(mdfihe.is_siblings_typhoid, FALSE) AS is_siblings_typhoid

FROM
    {{ ref("bse_dbo_mdata_bp") }} AS mdbp

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_patient_obs_gynae") }} AS mdobs
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_glucose_hb") }} AS mdghb
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_followup_date") }} AS mdfd 
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_height_weight") }}  AS mdhw
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("stg_cor_mdata_treatment_suggestion_extended") }} AS mdtse 
        USING(patient_id, collected_date)
    
    FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_cc_details_extended") }} AS mdpccde
        USING(patient_id, collected_date)
    
    FULL OUTER JOIN {{ ref("stg_cor_mdata_patient_vaccine_extended") }} AS mdpve
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("stg_cor_mdata_rx_details_extended") }} AS mdrxde
        USING(patient_id, collected_date)
    
    FULL OUTER JOIN {{ ref("stg_cor_mdata_provisional_diagnosis_extended") }} AS mdpde
        USING(patient_id, collected_date)
    
    FULL OUTER JOIN {{ ref("stg_cor_mdata_family_illness_history_extended") }} AS mdfihe
        USING(patient_id, collected_date)

