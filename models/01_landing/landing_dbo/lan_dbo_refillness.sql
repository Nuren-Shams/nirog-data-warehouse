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
            "`refillness`.`createdate`",
            "`refillness`.`createuser`",
            "`refillness`.`description`",
            "`refillness`.`familyho`",
            "`refillness`.`hoillness`",
            "`refillness`.`illnesscode`",
            "`refillness`.`illnessid`",
            "`refillness`.`orgid`",
            "`refillness`.`sortorder`",
            "`refillness`.`status`",
            "`refillness`.`updatedate`",
            "`refillness`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refillness`.`createdate`,
    `refillness`.`createuser`,
    `refillness`.`description`,
    `refillness`.`familyho`,
    `refillness`.`hoillness`,
    `refillness`.`illnesscode`,
    `refillness`.`illnessid`,
    `refillness`.`orgid`,
    `refillness`.`sortorder`,
    `refillness`.`status`,
    `refillness`.`updatedate`,
    `refillness`.`updateuser`

FROM
    {{ source("bay_dbo", "refillness") }} AS `refillness`
