{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn_dm", "branch"]
    )
-}}

SELECT
    h.district_name,
    h.upazila_name,
    h.union_name,
    h.health_center_name,
    h.barcode_prefix,
    h.total_registered_patients,
    h.total_htn_diagnosed_patients,
    d.total_dm_diagnosed_patients
FROM
    {{ ref('stg_agg_htn_info') }} AS h
LEFT JOIN
    {{ ref('stg_agg_dm_info') }} AS d
    ON
        h.district_name = d.district_name
        AND h.upazila_name = d.upazila_name
        AND h.union_name = d.union_name
        AND h.health_center_name = d.health_center_name
        AND h.barcode_prefix = d.barcode_prefix
        AND h.total_registered_patients = d.total_registered_patients
ORDER BY
    h.district_name,
    h.upazila_name,
    h.union_name,
    h.health_center_name
