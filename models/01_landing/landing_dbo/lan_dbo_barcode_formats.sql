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
            "barcode_formats.barcode_community_clinic",
            "barcode_formats.barcode_district",
            "barcode_formats.barcode_number",
            "barcode_formats.barcode_prefix",
            "barcode_formats.barcode_union",
            "barcode_formats.barcode_upazila",
            "barcode_formats.created_at",
            "barcode_formats.created_by",
            "barcode_formats.id",
            "barcode_formats.status",
            "barcode_formats.updated_at",
            "barcode_formats.updated_by"
        ])
    }} AS ingestion_sk,
    barcode_formats.barcode_community_clinic,
    barcode_formats.barcode_district,
    barcode_formats.barcode_number,
    barcode_formats.barcode_prefix,
    barcode_formats.barcode_union,
    barcode_formats.barcode_upazila,
    barcode_formats.created_at,
    barcode_formats.created_by,
    barcode_formats.id,
    barcode_formats.status,
    barcode_formats.updated_at,
    barcode_formats.updated_by

FROM
    {{ source("bay_dbo", "barcode_formats") }} AS barcode_formats
