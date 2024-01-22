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
            "`district_id`",
            "`id`",
            "`name`",
            "`url`"
        ])
    }} AS `ingestion_sk`,
    `bn_name`,
    `district_id`,
    `id`,
    `name`,
    `url`

FROM
    {{ source("bay_dbo", "upazilas") }}
