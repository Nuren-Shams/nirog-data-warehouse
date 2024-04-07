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
            "`customer_groups`.`created_at`",
            "`customer_groups`.`created_by`",
            "`customer_groups`.`group_name`",
            "`customer_groups`.`id`",
            "`customer_groups`.`percentage`",
            "`customer_groups`.`status`",
            "`customer_groups`.`updated_at`",
            "`customer_groups`.`updated_by`"
        ])
    }} AS `ingestion_sk`,
    `customer_groups`.`created_at`,
    `customer_groups`.`created_by`,
    `customer_groups`.`group_name`,
    `customer_groups`.`id`,
    `customer_groups`.`percentage`,
    `customer_groups`.`status`,
    `customer_groups`.`updated_at`,
    `customer_groups`.`updated_by`

FROM
    {{ source("bay_dbo", "customer_groups") }} AS `customer_groups`
