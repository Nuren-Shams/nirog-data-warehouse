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
            "`upazilas`.`bn_name`",
            "`upazilas`.`district_id`",
            "`upazilas`.`id`",
            "`upazilas`.`name`",
            "`upazilas`.`url`"
        ])
    }} AS `ingestion_sk`,
    `upazilas`.`bn_name`,
    `upazilas`.`district_id`,
    `upazilas`.`id`,
    `upazilas`.`name`,
    `upazilas`.`url`

FROM
    {{ source("bay_dbo", "upazilas") }} AS `upazilas`
