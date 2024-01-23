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
            "`failed_jobs`.`connection`",
            "`failed_jobs`.`exception`",
            "`failed_jobs`.`failed_at`",
            "`failed_jobs`.`id`",
            "`failed_jobs`.`payload`",
            "`failed_jobs`.`queue`"
        ])
    }} AS `ingestion_sk`,
    `failed_jobs`.`connection`,
    `failed_jobs`.`exception`,
    `failed_jobs`.`failed_at`,
    `failed_jobs`.`id`,
    `failed_jobs`.`payload`,
    `failed_jobs`.`queue`

FROM
    {{ source("bay_dbo", "failed_jobs") }} AS `failed_jobs`
