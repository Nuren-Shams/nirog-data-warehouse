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
            "`district`.`createdate`",
            "`district`.`createuser`",
            "`district`.`districtname`",
            "`district`.`divisionid`",
            "`district`.`id`",
            "`district`.`orgid`",
            "`district`.`shortname`",
            "`district`.`status`",
            "`district`.`updatedate`",
            "`district`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `district`.`createdate`,
    `district`.`createuser`,
    `district`.`districtname`,
    `district`.`divisionid`,
    `district`.`id`,
    `district`.`orgid`,
    `district`.`shortname`,
    `district`.`status`,
    `district`.`updatedate`,
    `district`.`updateuser`

FROM
    {{ source("bay_dbo", "district") }} AS `district`
