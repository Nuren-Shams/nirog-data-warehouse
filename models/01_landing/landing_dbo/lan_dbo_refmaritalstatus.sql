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
            "`refmaritalstatus`.`createdate`",
            "`refmaritalstatus`.`createuser`",
            "`refmaritalstatus`.`description`",
            "`refmaritalstatus`.`maritalstatuscode`",
            "`refmaritalstatus`.`maritalstatusid`",
            "`refmaritalstatus`.`orgid`",
            "`refmaritalstatus`.`sortorder`",
            "`refmaritalstatus`.`status`",
            "`refmaritalstatus`.`updatedate`",
            "`refmaritalstatus`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refmaritalstatus`.`createdate`,
    `refmaritalstatus`.`createuser`,
    `refmaritalstatus`.`description`,
    `refmaritalstatus`.`maritalstatuscode`,
    `refmaritalstatus`.`maritalstatusid`,
    `refmaritalstatus`.`orgid`,
    `refmaritalstatus`.`sortorder`,
    `refmaritalstatus`.`status`,
    `refmaritalstatus`.`updatedate`,
    `refmaritalstatus`.`updateuser`

FROM
    {{ source("bay_dbo", "refmaritalstatus") }} AS `refmaritalstatus`
