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
            "`barcode_community_clinic`",
            "`barcode_district`",
            "`barcode_number`",
            "`barcode_prefix`",
            "`barcode_union`",
            "`barcode_upazila`",
            "`created_at`",
            "`created_by`",
            "`id`",
            "`status`",
            "`updated_at`",
            "`updated_by`"
        ])
    }} AS `ingestion_sk`,
    `barcode_community_clinic`,
    `barcode_district`,
    `barcode_number`,
    `barcode_prefix`,
    `barcode_union`,
    `barcode_upazila`,
    `created_at`,
    `created_by`,
    `id`,
    `status`,
    `updated_at`,
    `updated_by`

FROM
    {{ source("bay_dbo", "barcode_formats") }}
