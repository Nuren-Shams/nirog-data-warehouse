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
            "`created_at`",
            "`created_by`",
            "`id`",
            "`name`",
            "`status`",
            "`updated_at`",
            "`updated_by`"
        ])
    }} AS `ingestion_sk`,
    `created_at`,
    `created_by`,
    `id`,
    `name`,
    `status`,
    `updated_at`,
    `updated_by`

FROM
    {{ source("bay_dbo", "patients") }}
