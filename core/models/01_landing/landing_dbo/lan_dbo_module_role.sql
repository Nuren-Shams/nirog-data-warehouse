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
            "`module_role`.`created_at`",
            "`module_role`.`id`",
            "`module_role`.`module_id`",
            "`module_role`.`role_id`",
            "`module_role`.`updated_at`"
        ])
    }} AS `ingestion_sk`,
    `module_role`.`created_at`,
    `module_role`.`id`,
    `module_role`.`module_id`,
    `module_role`.`role_id`,
    `module_role`.`updated_at`

FROM
    {{ source("bay_dbo", "module_role") }} AS `module_role`
