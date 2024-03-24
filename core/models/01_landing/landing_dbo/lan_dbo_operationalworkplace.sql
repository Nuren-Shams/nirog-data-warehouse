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
            "`operationalworkplace`.`createdate`",
            "`operationalworkplace`.`createuser`",
            "`operationalworkplace`.`id`",
            "`operationalworkplace`.`status`",
            "`operationalworkplace`.`updatedate`",
            "`operationalworkplace`.`updateuser`",
            "`operationalworkplace`.`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `operationalworkplace`.`createdate`,
    `operationalworkplace`.`createuser`,
    `operationalworkplace`.`id`,
    `operationalworkplace`.`status`,
    `operationalworkplace`.`updatedate`,
    `operationalworkplace`.`updateuser`,
    `operationalworkplace`.`workplaceid`

FROM
    {{ source("bay_dbo", "operationalworkplace") }} AS `operationalworkplace`
