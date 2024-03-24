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
            "`module`.`createdate`",
            "`module`.`createuser`",
            "`module`.`description`",
            "`module`.`modulecode`",
            "`module`.`moduleid`",
            "`module`.`orgid`",
            "`module`.`sortorder`",
            "`module`.`status`",
            "`module`.`updatedate`",
            "`module`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `module`.`createdate`,
    `module`.`createuser`,
    `module`.`description`,
    `module`.`modulecode`,
    `module`.`moduleid`,
    `module`.`orgid`,
    `module`.`sortorder`,
    `module`.`status`,
    `module`.`updatedate`,
    `module`.`updateuser`

FROM
    {{ source("bay_dbo", "module") }} AS `module`
