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
            "`permissions`.`created_at`",
            "`permissions`.`id`",
            "`permissions`.`module_id`",
            "`permissions`.`name`",
            "`permissions`.`slug`",
            "`permissions`.`updated_at`"
        ])
    }} AS `ingestion_sk`,
    `permissions`.`created_at`,
    `permissions`.`id`,
    `permissions`.`module_id`,
    `permissions`.`name`,
    `permissions`.`slug`,
    `permissions`.`updated_at`

FROM
    {{ source("bay_dbo", "permissions") }} AS `permissions`
