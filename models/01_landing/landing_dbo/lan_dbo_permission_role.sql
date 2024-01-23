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
            "`permission_role`.`created_at`",
            "`permission_role`.`id`",
            "`permission_role`.`permission_id`",
            "`permission_role`.`role_id`",
            "`permission_role`.`updated_at`"
        ])
    }} AS `ingestion_sk`,
    `permission_role`.`created_at`,
    `permission_role`.`id`,
    `permission_role`.`permission_id`,
    `permission_role`.`role_id`,
    `permission_role`.`updated_at`

FROM
    {{ source("bay_dbo", "permission_role") }} AS `permission_role`
