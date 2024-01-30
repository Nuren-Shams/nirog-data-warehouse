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
            "`password_resets`.`created_at`",
            "`password_resets`.`email`",
            "`password_resets`.`token`"
        ])
    }} AS `ingestion_sk`,
    `password_resets`.`created_at`,
    `password_resets`.`email`,
    `password_resets`.`token`

FROM
    {{ source("bay_dbo", "password_resets") }} AS `password_resets`
