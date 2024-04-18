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
            "`refdmhtndrug`.`create_date`",
            "`refdmhtndrug`.`create_user`",
            "`refdmhtndrug`.`formation`",
            "`refdmhtndrug`.`generic_name`",
            "`refdmhtndrug`.`id`",
            "`refdmhtndrug`.`org_id`",
            "`refdmhtndrug`.`strength`",
            "`refdmhtndrug`.`trade_name`",
            "`refdmhtndrug`.`type`",
            "`refdmhtndrug`.`update_date`",
            "`refdmhtndrug`.`update_user`"
        ])
    }} AS `ingestion_sk`,
    `refdmhtndrug`.`create_date`,
    `refdmhtndrug`.`create_user`,
    `refdmhtndrug`.`formation`,
    `refdmhtndrug`.`generic_name`,
    `refdmhtndrug`.`id`,
    `refdmhtndrug`.`org_id`,
    `refdmhtndrug`.`strength`,
    `refdmhtndrug`.`trade_name`,
    `refdmhtndrug`.`type`,
    `refdmhtndrug`.`update_date`,
    `refdmhtndrug`.`update_user`

FROM
    {{ source("bay_dbo", "refdmhtndrug") }} AS `refdmhtndrug`
