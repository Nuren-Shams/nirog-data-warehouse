{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "social_behavior", "extended"]
    )
-}}

SELECT
    mdsb.patient_id,
    mdsb.collected_date,
    CONCAT("Social Behavior: ", STRING_AGG(DISTINCT social_behavior_code, ", " ORDER BY social_behavior_code)) AS social_behavior_history

FROM
    {{ ref("bse_dbo_mdata_social_behavior") }} AS mdsb

LEFT JOIN {{ ref("bse_dbo_ref_social_behavior") }} AS rsb
    ON
        mdsb.social_behavior_id = rsb.social_behavior_id

GROUP BY
    mdsb.patient_id,
    mdsb.collected_date
