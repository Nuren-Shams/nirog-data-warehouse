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
            "`current_utc_offset`",
            "`is_currently_dst`",
            "`name`"
        ])
    }} AS `ingestion_sk`,
    `current_utc_offset`,
    `is_currently_dst`,
    `name`

FROM
    {{ source("bay_dbo", "reftimezoneinfo") }}
