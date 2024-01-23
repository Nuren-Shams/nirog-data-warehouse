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
            "mdatacc_barcodes.address",
            "mdatacc_barcodes.created_at",
            "mdatacc_barcodes.created_by",
            "mdatacc_barcodes.id",
            "mdatacc_barcodes.mdata_barcode_generate",
            "mdatacc_barcodes.mdata_barcode_number",
            "mdatacc_barcodes.mdata_barcode_prefix",
            "mdatacc_barcodes.mdata_barcode_prefix_number",
            "mdatacc_barcodes.mdata_barcode_status",
            "mdatacc_barcodes.status",
            "mdatacc_barcodes.updated_at",
            "mdatacc_barcodes.updated_by"
        ])
    }} AS ingestion_sk,
    mdatacc_barcodes.address,
    mdatacc_barcodes.created_at,
    mdatacc_barcodes.created_by,
    mdatacc_barcodes.id,
    mdatacc_barcodes.mdata_barcode_generate,
    mdatacc_barcodes.mdata_barcode_number,
    mdatacc_barcodes.mdata_barcode_prefix,
    mdatacc_barcodes.mdata_barcode_prefix_number,
    mdatacc_barcodes.mdata_barcode_status,
    mdatacc_barcodes.status,
    mdatacc_barcodes.updated_at,
    mdatacc_barcodes.updated_by

FROM
    {{ source("bay_dbo", "mdatacc_barcodes") }} AS mdatacc_barcodes
