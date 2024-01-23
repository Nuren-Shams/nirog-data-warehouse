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
            "`workplace`.`createdate`",
            "`workplace`.`createuser`",
            "`workplace`.`description`",
            "`workplace`.`districtid`",
            "`workplace`.`orgid`",
            "`workplace`.`status`",
            "`workplace`.`updatedate`",
            "`workplace`.`updateuser`",
            "`workplace`.`workplacecode`",
            "`workplace`.`workplaceid`",
            "`workplace`.`workplacename`"
        ])
    }} AS `ingestion_sk`,
    `workplace`.`createdate`,
    `workplace`.`createuser`,
    `workplace`.`description`,
    `workplace`.`districtid`,
    `workplace`.`orgid`,
    `workplace`.`status`,
    `workplace`.`updatedate`,
    `workplace`.`updateuser`,
    `workplace`.`workplacecode`,
    `workplace`.`workplaceid`,
    `workplace`.`workplacename`

FROM
    {{ source("bay_dbo", "workplace") }} AS `workplace`
