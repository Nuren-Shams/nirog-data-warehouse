{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "agg", "htn"]
    )
-}}


{%- call set_sql_header(config) -%}
    CREATE TEMPORARY FUNCTION array_membership(med_arr ARRAY<STRING>, rx_arr ARRAY<STRING>) AS ((
        SELECT
            CASE
                WHEN ARRAY_LENGTH(ARRAY_AGG(med)) IS NULL THEN FALSE
                WHEN ARRAY_LENGTH(ARRAY_AGG(rx)) IS NULL THEN FALSE
                ELSE TRUE
            END AS common
        FROM
            UNNEST(med_arr) AS med
            CROSS JOIN UNNEST(rx_arr) AS rx
        WHERE UPPER(rx) LIKE CONCAT("%", UPPER(med), "%")
    ));
{%- endcall -%}

WITH anti_htn_meds AS (
    SELECT
        type,
        ARRAY_AGG(DISTINCT trade_name) AS trade_names,
        ARRAY_AGG(DISTINCT generic_name) AS generic_names

    FROM
        {{ ref("bse_ext_med__htn_dm__cxb_noakhali") }}

    WHERE
        TRUE
        AND type = "ANTI HTN"

    GROUP BY
        type
),

registered_patients AS (
    SELECT
        DATE(created_at) AS period_start_date,
        health_center_name,
        mdata_barcode_prefix AS barcode_prefix,
        district_name,
        upazila_name,
        union_name,
        COUNT(patient_id) AS registered_patients

    FROM
        {{ ref("stg_cor_patient_extended") }}

    WHERE
        TRUE
        AND registration_id IS NOT NULL

    GROUP BY
        period_start_date,
        health_center_name,
        barcode_prefix,
        district_name,
        upazila_name,
        union_name
),

screened_patients AS (
    SELECT
        DATE(mdata.collected_date) AS period_start_date,
        p.health_center_name,
        p.mdata_barcode_prefix AS barcode_prefix,
        p.district_name,
        p.upazila_name,
        p.union_name,
        -- Patients Screened for HTN
        COUNT(mdata.patient_id) AS htn_screened_patients,
        COUNT(IF(p.gender_code = "MALE", mdata.patient_id, NULL)) AS htn_screened_male_patients,
        COUNT(IF(p.gender_code = "FEMALE", mdata.patient_id, NULL)) AS htn_screened_female_patients,
        -- Patients Diagnosed with HTN
        COUNT(CASE WHEN mdata.bp_systolic > 130 OR mdata.bp_diastolic > 80 THEN mdata.patient_id END) AS htn_diagnosed_patients,
        COUNT(CASE WHEN (mdata.bp_systolic > 130 OR mdata.bp_diastolic > 80) AND p.gender_code = "MALE" THEN mdata.patient_id END) AS htn_diagnosed_male_patients,
        COUNT(CASE WHEN (mdata.bp_systolic > 130 OR mdata.bp_diastolic > 80) AND p.gender_code = "FEMALE" THEN mdata.patient_id END) AS htn_diagnosed_female_patients,
        -- Patients NOT Diagnosed with HTN
        COUNT(CASE WHEN mdata.bp_systolic <= 130 AND mdata.bp_diastolic <= 80 THEN mdata.patient_id END) AS non_htn_patients,
        COUNT(CASE WHEN mdata.bp_systolic <= 130 AND mdata.bp_diastolic <= 80 AND p.gender_code = "MALE" THEN mdata.patient_id END) AS non_htn_male_patients,
        COUNT(CASE WHEN mdata.bp_systolic <= 130 AND mdata.bp_diastolic <= 80 AND p.gender_code = "FEMALE" THEN mdata.patient_id END) AS non_htn_female_patients,

        COUNT(CASE WHEN ARRAY_MEMBERSHIP(ahtnm.trade_names, SPLIT(mdata.rx_details, ",\n")) OR ARRAY_MEMBERSHIP(ahtnm.generic_names, SPLIT(mdata.rx_details, ",\n")) THEN mdata.patient_id END) AS medication_received_patients,
        COUNT(CASE WHEN (mdata.bp_systolic > 130 OR mdata.bp_diastolic > 80) AND mdata.followup_date IS NOT NULL AND DATE_DIFF(mdata.next_collected_date, mdata.followup_date, DAY) > 14 THEN mdata.patient_id END) AS lost_followup_patients

    FROM
        {{ ref("stg_cor_mdata_super_table") }} AS mdata

    LEFT JOIN {{ ref("stg_cor_patient_extended") }} AS p
        ON
            mdata.patient_id = p.patient_id

    CROSS JOIN
        anti_htn_meds AS ahtnm

    WHERE
        TRUE
        AND mdata.bp_systolic IS NOT NULL
        AND mdata.bp_diastolic IS NOT NULL
        AND p.registration_id IS NOT NULL

    GROUP BY
        period_start_date,
        p.health_center_name,
        barcode_prefix,
        p.district_name,
        p.upazila_name,
        p.union_name
)

SELECT
    period_start_date,
    health_center_name,
    barcode_prefix,
    district_name,
    upazila_name,
    union_name,
    COALESCE(rp.registered_patients, 0) AS registered_patients,
    COALESCE(sp.htn_screened_patients, 0) AS htn_screened_patients,
    COALESCE(sp.htn_screened_male_patients, 0) AS htn_screened_male_patients,
    COALESCE(sp.htn_screened_female_patients, 0) AS htn_screened_female_patients,
    COALESCE(sp.htn_diagnosed_patients, 0) AS htn_diagnosed_patients,
    COALESCE(sp.non_htn_patients, 0) AS non_htn_patients,
    COALESCE(sp.medication_received_patients, 0) AS medication_received_patients,
    COALESCE(sp.lost_followup_patients, 0) AS lost_followup_patients
    -- , SUM(IFNULL(rp.registered_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_registered_patients
    -- , SUM(IFNULL(sp.htn_screened_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_htn_screened_patients
    -- , SUM(IFNULL(sp.non_htn_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_non_htn_patients
    -- , SUM(IFNULL(sp.medication_received_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_medication_received_patients
    -- , SUM(IFNULL(sp.lost_followup_patients, 0)) OVER(previous_all_days_cumulative) AS cumulative_lost_followup_patients

FROM
    registered_patients AS rp

FULL OUTER JOIN screened_patients AS sp
    USING (
        period_start_date,
        health_center_name,
        barcode_prefix,
        district_name,
        upazila_name,
        union_name
    )
WHERE
    1 = 1
    AND health_center_name IS NOT NULL

WINDOW
    previous_all_days_cumulative AS (
        PARTITION BY rp.health_center_name, rp.district_name, rp.upazila_name, rp.union_name
        ORDER BY rp.period_start_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
