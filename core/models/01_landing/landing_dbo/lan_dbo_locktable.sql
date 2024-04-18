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
            "`locktable`.`id`",
            "`locktable`.`lockind`",
            "`locktable`.`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `locktable`.`id`,
    `locktable`.`lockind`,
    `locktable`.`workplaceid`

FROM
    {{ source("bay_dbo", "locktable") }} AS `locktable`
