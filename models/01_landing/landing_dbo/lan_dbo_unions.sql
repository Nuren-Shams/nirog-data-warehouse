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
            "`bn_name`",
            "`id`",
            "`name`",
            "`upazilla_id`",
            "`url`"
        ])
    }} AS `ingestion_sk`,
    `bn_name`,
    `id`,
    `name`,
    `upazilla_id`,
    `url`

FROM
    {{ source("bay_dbo", "unions") }}
