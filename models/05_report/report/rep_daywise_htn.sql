{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn"]
    )
-}}

SELECT
    mdata.patient_id
    , p.given_name
    , p.family_name
    , p.created_at
    , p.id_number
    , p.id_type
    , mdata.collected_date
    , mdata.is_pregnant
    , COALESCE(mdata.bp_systolic_2, mdata.bp_systolic_1) AS bp_systolic
    , COALESCE(mdata.bp_diastolic_2, mdata.bp_diastolic_1) AS bp_diastolic

FROM
    {{ ref("stg_cor_mdata_super_table") }} AS mdata

    LEFT JOIN {{ ref("bse_dbo_patient") }} AS p
        ON
            mdata.patient_id = p.patient_id
