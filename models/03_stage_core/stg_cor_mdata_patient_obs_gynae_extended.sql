{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_obs_gynae", "extended"]
    )
-}}


SELECT 
    mdobs.patient_id 
    , mdobs.collected_date
    , IFNULL(mdobs.is_pregnant, false) AS is_pregnant
    , mdobs.gravida
    , mdobs.still_birth 
    , mdobs.miscarraige_or_abortion
    , mdobs.mr
    , mdobs.living_birth 
    , mdobs.living_male
    , mdobs.living_female 
    , mdobs.child_mortality_0_to_1
    , mdobs.child_mortality_below_5
    , mdobs.child_mortality_over_5
    , mdobs.lmp 
    , mdobs.other_contraception_method
    , mdobs.other_menstruation_product
    , mdobs.other_menstruation_product_usage_time
    , rcm.contraception_method_code
    , rmp.menstruation_product_code

FROM 
    {{ ref("bse_dbo_mdata_patient_obs_gynae") }} AS mdobs

    LEFT JOIN {{ ref("bse_dbo_ref_contraception_method") }} AS rcm
        ON 
            mdobs.contraception_method_id = rcm.contraception_method_id
    
    LEFT JOIN {{ ref("bse_dbo_ref_menstruation_product") }} AS rmp
        ON 
            mdobs.menstruation_product_id = rmp.menstruation_product_id