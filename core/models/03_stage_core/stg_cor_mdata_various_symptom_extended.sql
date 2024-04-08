{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "mdata", "various_symptom", "extended"]
    )
-}}

WITH mdata_various_symptom AS (
    SELECT
        patient_id,
        collected_date,
        IF(cough_greater_than_month = "YES", "COUGH GREATER THAN MONTH", NULL) AS cough_greater_than_month_processed,
        IF(lgerf = "YES", "LGERF", NULL) AS lgerf_processed,
        IF(night_sweat = "YES", "NIGHT SWEAT", NULL) AS night_sweat_processed,
        IF(weight_loss = "YES", "WEIGHT LOSS", NULL) AS weight_loss_processed

    FROM
        {{ ref("bse_dbo_mdata_various_symptom") }}
)

SELECT
    patient_id,
    collected_date,
    tb_screening

FROM
    mdata_various_symptom UNPIVOT (
    tb_screening FOR _ IN (
        cough_greater_than_month_processed,
        lgerf_processed,
        night_sweat_processed,
        weight_loss_processed
    )
)
