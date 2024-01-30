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
            "`division`.`createdate`",
            "`division`.`createuser`",
            "`division`.`divisionname`",
            "`division`.`id`",
            "`division`.`orgid`",
            "`division`.`shortname`",
            "`division`.`status`",
            "`division`.`updatedate`",
            "`division`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `division`.`createdate`,
    `division`.`createuser`,
    `division`.`divisionname`,
    `division`.`id`,
    `division`.`orgid`,
    `division`.`shortname`,
    `division`.`status`,
    `division`.`updatedate`,
    `division`.`updateuser`

FROM
    {{ source("bay_dbo", "division") }} AS `division`
