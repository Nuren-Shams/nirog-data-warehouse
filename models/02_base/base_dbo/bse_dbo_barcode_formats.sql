{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(barcode_community_clinic) IN ("NONE", ""), NULL, UPPER(barcode_community_clinic)) AS barcode_community_clinic_id,
    IF(UPPER(barcode_number) IN ("NONE", ""), NULL, UPPER(barcode_number)) AS barcode_number,
    IF(UPPER(barcode_prefix) IN ("NONE", ""), NULL, UPPER(barcode_prefix)) AS barcode_prefix,
    SAFE_CAST(barcode_district AS INT64) AS district_id_int,
    SAFE_CAST(barcode_union AS INT64) AS union_id_int,
    SAFE_CAST(barcode_upazila AS INT64) AS upazila_id_int,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", created_at) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", updated_at) AS updated_at,

FROM
    {{ ref("lan_dbo_barcode_formats") }}

QUALIFY ROW_NUMBER() OVER(PARTITION BY barcode_community_clinic_id ORDER BY updated_at DESC) = 1
