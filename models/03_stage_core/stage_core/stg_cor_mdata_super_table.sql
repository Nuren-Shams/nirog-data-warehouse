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
    patient_id
    , collected_date
    , IFNULL(obs.is_pregnant, false) AS is_pregnant
    , bp.bp_systolic_1
    , bp.bp_systolic_2
    , bp.bp_diastolic_1
    , bp.bp_diastolic_2
    , COALESCE(bp.bp_systolic_2, bp.bp_systolic_1) AS bp_systolic 
    , COALESCE(bp.bp_diastolic_2, bp.bp_diastolic_1) AS bp_diastolic
    , hb.fbg
    , hb.rbg
    , rx.prescribed_rx
    , fd.followup_date_id
    , fd.followup_date
    , LEAD(collected_date, 1, "2050-01-01") OVER(PARTITION BY patient_id ORDER BY collected_date ASC) AS next_collected_date

FROM
    {{ ref("bse_dbo_mdata_bp") }} AS bp

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_patient_obs_gynae") }} AS obs
        USING(patient_id, collected_date)

    FULL OUTER JOIN {{ ref("bse_dbo_mdata_glucose_hb") }} AS hb
        USING(patient_id, collected_date)

    FULL OUTER JOIN mdata_rx_details AS rx
        USING(patient_id, collected_date)

    FULL OUTER JOIN{{ ref("bse_dbo_mdata_followup_date") }} AS fd 
        USING(patient_id, collected_date)
