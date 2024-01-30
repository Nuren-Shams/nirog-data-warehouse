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
            "`sessions`.`id`",
            "`sessions`.`ip_address`",
            "`sessions`.`last_activity`",
            "`sessions`.`payload`",
            "`sessions`.`user_agent`",
            "`sessions`.`user_id`"
        ])
    }} AS `ingestion_sk`,
    `sessions`.`id`,
    `sessions`.`ip_address`,
    `sessions`.`last_activity`,
    `sessions`.`payload`,
    `sessions`.`user_agent`,
    `sessions`.`user_id`

FROM
    {{ source("bay_dbo", "sessions") }} AS `sessions`
