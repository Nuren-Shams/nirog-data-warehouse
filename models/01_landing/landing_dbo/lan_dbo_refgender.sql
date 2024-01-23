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
            "`refgender`.`createdate`",
            "`refgender`.`createuser`",
            "`refgender`.`description`",
            "`refgender`.`gendercode`",
            "`refgender`.`genderid`",
            "`refgender`.`orgid`",
            "`refgender`.`sortorder`",
            "`refgender`.`status`",
            "`refgender`.`updatedate`",
            "`refgender`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refgender`.`createdate`,
    `refgender`.`createuser`,
    `refgender`.`description`,
    `refgender`.`gendercode`,
    `refgender`.`genderid`,
    `refgender`.`orgid`,
    `refgender`.`sortorder`,
    `refgender`.`status`,
    `refgender`.`updatedate`,
    `refgender`.`updateuser`

FROM
    {{ source("bay_dbo", "refgender") }} AS `refgender`
