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
            "`districts`.`bn_name`",
            "`districts`.`division_id`",
            "`districts`.`id`",
            "`districts`.`lat`",
            "`districts`.`lon`",
            "`districts`.`name`",
            "`districts`.`url`"
        ])
    }} AS `ingestion_sk`,
    `districts`.`bn_name`,
    `districts`.`division_id`,
    `districts`.`id`,
    `districts`.`lat`,
    `districts`.`lon`,
    `districts`.`name`,
    `districts`.`url`

FROM
    {{ source("bay_dbo", "districts") }} AS `districts`
