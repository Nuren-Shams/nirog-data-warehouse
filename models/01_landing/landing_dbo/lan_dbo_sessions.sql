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
            "`id`",
            "`ip_address`",
            "`last_activity`",
            "`payload`",
            "`user_agent`",
            "`user_id`"
        ])
    }} AS `ingestion_sk`,
    `id`,
    `ip_address`,
    `last_activity`,
    `payload`,
    `user_agent`,
    `user_id`

FROM
    {{ source("bay_dbo", "sessions") }}
