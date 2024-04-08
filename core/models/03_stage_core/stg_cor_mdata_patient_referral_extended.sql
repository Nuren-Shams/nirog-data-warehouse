{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "patient_referral", "extended"]
    )
-}}


SELECT
    mdpr.patient_id,
    mdpr.collected_date,
    mdpr.health_center_id AS referred_to_health_center_id,
    hc.health_center_name AS referred_to_health_center_name

FROM
    {{ ref("bse_dbo_mdata_patient_referral") }} AS mdpr

LEFT JOIN {{ ref("bse_dbo_health_center") }} AS hc
    ON
        mdpr.health_center_id = hc.health_center_id
