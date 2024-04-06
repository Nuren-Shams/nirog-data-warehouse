{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "report", "htn_dm", "branch"]
    )
-}}

SELECT
    district_name,
    upazila_name,
    union_name,
    health_center_name,
    barcode_prefix,
    period_start_date,
    registered_patients,
    h.htn_screened_patients,
    h.htn_diagnosed_patients,
    d.dm_screened_patients,
    d.dm_diagnosed_patients
FROM
    {{ ref('stg_agg_htn_info_daily') }} AS h
FULL OUTER JOIN
    {{ ref('stg_agg_dm_info_daily') }} AS d
    USING(district_name, upazila_name, union_name, health_center_name, barcode_prefix, registered_patients, period_start_date)
    -- ON
    --     h.district_name = d.district_name
    --     AND h.upazila_name = d.upazila_name
    --     AND h.union_name = d.union_name
    --     AND h.health_center_name = d.health_center_name
    --     AND h.barcode_prefix = d.barcode_prefix
    --     AND h.total_registered_patients = d.total_registered_patients
ORDER BY
    district_name,
    upazila_name,
    union_name,
    health_center_name
