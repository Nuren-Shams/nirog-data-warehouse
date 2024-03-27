{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "patient", "extended"]
    )
-}}


SELECT
    p.patient_id,
    p.registration_id,
    p.id_type,
    p.id_number,
    p.patient_code,
    p.given_name,
    p.family_name,
    p.gender_id,
    rg.gender_code,
    rg.description AS gender_description,
    p.birth_date,
    p.age,
    p.cell_number,
    p.joined_at,
    p.created_at,
    p.updated_at,
    p.workplace_id,
    p.workplace_branch_id,
    p.barcode,
    wp.workplace_code,
    wp.workplace_name,
    wp.district_id,
    wpb.workplace_branch_code,
    wpb.description AS workplace_branch_description,
    bc.mdata_barcode_prefix_number,
    bc.mdata_barcode_prefix,
    hce.health_center_name,
    hce.district_name,
    hce.upazila_name,
    hce.union_name

FROM
    {{ ref("bse_dbo_patient") }} AS p

LEFT OUTER JOIN {{ ref("bse_dbo_workplace") }} AS wp
    ON
        p.workplace_id = wp.workplace_id

LEFT OUTER JOIN {{ ref("bse_dbo_workplace_branch") }} AS wpb
    ON
        p.workplace_branch_id = wpb.workplace_branch_id

LEFT OUTER JOIN {{ ref("bse_dbo_mdata_cc_barcodes") }} AS bc
    ON
        p.patient_code = bc.mdata_barcode_prefix_number

LEFT OUTER JOIN {{ ref("stg_cor_health_center_extended") }} AS hce
    ON
        bc.mdata_barcode_prefix = hce.barcode_prefix

LEFT OUTER JOIN {{ ref("bse_dbo_ref_gender") }} AS rg
    ON
        p.gender_id = rg.gender_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY p.patient_code ORDER BY p.created_at ASC) = 1
