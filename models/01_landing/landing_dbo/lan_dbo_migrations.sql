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
            "`batch`",
            "`id`",
            "`migration`"
        ])
    }} AS `ingestion_sk`,
    `batch`,
    `id`,
    `migration`

FROM
    {{ source("bay_dbo", "migrations") }}
