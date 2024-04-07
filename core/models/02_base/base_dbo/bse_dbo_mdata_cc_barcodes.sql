{{-
    config(
        materialized = "table",
        tags = ["execute_daily"]
    )
-}}

SELECT
    IF(UPPER(address) IN ("NONE", ""), NULL, UPPER(address)) AS address,
    IF(UPPER(mdata_barcode_number) IN ("NONE", ""), NULL, UPPER(mdata_barcode_number)) AS mdata_barcode_number,
    IF(UPPER(mdata_barcode_prefix) IN ("NONE", ""), NULL, UPPER(mdata_barcode_prefix)) AS mdata_barcode_prefix,
    IF(UPPER(mdata_barcode_prefix_number) IN ("NONE", ""), NULL, UPPER(mdata_barcode_prefix_number)) AS mdata_barcode_prefix_number,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", created_at) AS created_at,
    SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", updated_at) AS updated_at

FROM
    {{ ref("lan_dbo_mdatacc_barcodes") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY mdata_barcode_prefix_number ORDER BY updated_at DESC) = 1
