{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "super_table"]
    )
-}}

WITH mdata_rx_details AS (
    SELECT 
        patient_id
        , collected_date
        , STRING_AGG(rx_name, ",\n") AS prescribed_rx
    FROM 
        {{ ref("bse_dbo_mdata_rx_details") }}
    GROUP BY
        patient_id 
        , collected_date
)

SELECT 
    bp.patient_id
    , collected_date
    , IFNULL(obs.is_pregnant, false) AS is_pregnant
    , bp.bp_systolic_1
    , bp.bp_systolic_2
    , bp.bp_diastolic_1
    , bp.bp_diastolic_2
    , hb.fbg
    , hb.rbg
    , rx.prescribed_rx

FROM
    {{ ref("bse_dbo_mdata_bp") }} AS bp

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_patient_obs_gynae") }} AS obs
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_glucose_hb") }} AS hb
        USING(patient_id, collected_date)

    FULL OUTER JOIN mdata_rx_details AS rx
        USING(patient_id, collected_date)
