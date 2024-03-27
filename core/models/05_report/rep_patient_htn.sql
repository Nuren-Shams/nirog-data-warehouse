{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn"]
    )
-}}

SELECT
    mdata.patient_id,
    p.given_name,
    p.family_name,
    p.created_at,
    p.id_number,
    p.id_type,
    p.patient_code,
    mdata.collected_date,
    mdata.is_pregnant,
    p.health_center_name,
    p.district_name,
    p.upazila_name,
    p.union_name,
    mdata.bp_systolic,
    mdata.bp_diastolic,
    mdata.rx_details AS prescribed_drugs

FROM
    {{ ref("stg_cor_mdata_super_table") }} AS mdata

LEFT JOIN {{ ref("stg_cor_patient_extended") }} AS p
    ON
        mdata.patient_id = p.patient_id

WHERE
    TRUE
    AND mdata.bp_systolic IS NOT NULL
    AND p.health_center_name IS NOT NULL
