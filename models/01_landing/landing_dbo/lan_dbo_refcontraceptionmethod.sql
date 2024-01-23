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
            "`refcontraceptionmethod`.`contraceptionmethodcode`",
            "`refcontraceptionmethod`.`contraceptionmethodid`",
            "`refcontraceptionmethod`.`createdate`",
            "`refcontraceptionmethod`.`createuser`",
            "`refcontraceptionmethod`.`description`",
            "`refcontraceptionmethod`.`orgid`",
            "`refcontraceptionmethod`.`sortorder`",
            "`refcontraceptionmethod`.`status`",
            "`refcontraceptionmethod`.`updatedate`",
            "`refcontraceptionmethod`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refcontraceptionmethod`.`contraceptionmethodcode`,
    `refcontraceptionmethod`.`contraceptionmethodid`,
    `refcontraceptionmethod`.`createdate`,
    `refcontraceptionmethod`.`createuser`,
    `refcontraceptionmethod`.`description`,
    `refcontraceptionmethod`.`orgid`,
    `refcontraceptionmethod`.`sortorder`,
    `refcontraceptionmethod`.`status`,
    `refcontraceptionmethod`.`updatedate`,
    `refcontraceptionmethod`.`updateuser`

FROM
    {{ source("bay_dbo", "refcontraceptionmethod") }} AS `refcontraceptionmethod`
