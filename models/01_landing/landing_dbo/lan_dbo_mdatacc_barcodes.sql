{{-
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
-}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "`address`",
            "`created_at`",
            "`created_by`",
            "`id`",
            "`mdata_barcode_generate`",
            "`mdata_barcode_number`",
            "`mdata_barcode_prefix`",
            "`mdata_barcode_prefix_number`",
            "`mdata_barcode_status`",
            "`status`",
            "`updated_at`",
            "`updated_by`"
        ])
    }} AS `ingestion_sk`,
    `address`,
    `created_at`,
    `created_by`,
    `id`,
    `mdata_barcode_generate`,
    `mdata_barcode_number`,
    `mdata_barcode_prefix`,
    `mdata_barcode_prefix_number`,
    `mdata_barcode_status`,
    `status`,
    `updated_at`,
    `updated_by`

FROM
    {{ source("bay_dbo", "mdatacc_barcodes") }}
