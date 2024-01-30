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
            "`modules`.`created_at`",
            "`modules`.`divider_title`",
            "`modules`.`icon_class`",
            "`modules`.`id`",
            "`modules`.`menu_id`",
            "`modules`.`module_name`",
            "`modules`.`order`",
            "`modules`.`parent_id`",
            "`modules`.`target`",
            "`modules`.`type`",
            "`modules`.`updated_at`",
            "`modules`.`url`"
        ])
    }} AS `ingestion_sk`,
    `modules`.`created_at`,
    `modules`.`divider_title`,
    `modules`.`icon_class`,
    `modules`.`id`,
    `modules`.`menu_id`,
    `modules`.`module_name`,
    `modules`.`order`,
    `modules`.`parent_id`,
    `modules`.`target`,
    `modules`.`type`,
    `modules`.`updated_at`,
    `modules`.`url`

FROM
    {{ source("bay_dbo", "modules") }} AS `modules`
