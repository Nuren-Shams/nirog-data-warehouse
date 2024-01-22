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
            "`created_at`",
            "`divider_title`",
            "`icon_class`",
            "`id`",
            "`menu_id`",
            "`module_name`",
            "`order`",
            "`parent_id`",
            "`target`",
            "`type`",
            "`updated_at`",
            "`url`"
        ])
    }} AS `ingestion_sk`,
    `created_at`,
    `divider_title`,
    `icon_class`,
    `id`,
    `menu_id`,
    `module_name`,
    `order`,
    `parent_id`,
    `target`,
    `type`,
    `updated_at`,
    `url`

FROM
    {{ source("bay_dbo", "modules") }}
