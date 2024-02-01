{{-
    config(
        materialized = "table",
        tags = ["execute_daily", "healthcenter", "extended"]
    )
-}}

SELECT 
    bf.barcode_community_clinic_id
    , hc.health_center_code
    , hc.health_center_name
    , hc.health_center_type
    , bf.district_id_int
    , bf.union_id_int 
    , bf.upazila_id_int
    , ae.district_name
    , ae.upazila_name 
    , ae.union_name 
    
FROM
    {{ ref("bse_dbo_barcode_formats") }} AS bf

JOIN 
    {{ ref("bse_dbo_health_center") }} AS hc
    ON 
        bf.barcode_community_clinic_id = hc.health_center_id

LEFT JOIN 
    {{ ref("stg_cor_area_extended") }} AS ae
    ON 
        bf.district_id_int = ae.district_id_int
        AND bf.union_id_int = ae.union_id_int
        AND bf.upazila_id_int = ae.upazila_id_int
