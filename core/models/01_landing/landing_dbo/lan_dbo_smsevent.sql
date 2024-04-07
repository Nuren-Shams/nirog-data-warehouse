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
            "`smsevent`.`createdate`",
            "`smsevent`.`createuser`",
            "`smsevent`.`orgid`",
            "`smsevent`.`smseventid`",
            "`smsevent`.`smseventname`",
            "`smsevent`.`smsmessagetemplateid`"
        ])
    }} AS `ingestion_sk`,
    `smsevent`.`createdate`,
    `smsevent`.`createuser`,
    `smsevent`.`orgid`,
    `smsevent`.`smseventid`,
    `smsevent`.`smseventname`,
    `smsevent`.`smsmessagetemplateid`

FROM
    {{ source("bay_dbo", "smsevent") }} AS `smsevent`
