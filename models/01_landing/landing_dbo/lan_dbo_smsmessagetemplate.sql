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
            "`smsmessagetemplate`.`createdate`",
            "`smsmessagetemplate`.`createuser`",
            "`smsmessagetemplate`.`orgid`",
            "`smsmessagetemplate`.`smsmessagetemplatebody`",
            "`smsmessagetemplate`.`smsmessagetemplateid`",
            "`smsmessagetemplate`.`smsmessagetemplatename`"
        ])
    }} AS `ingestion_sk`,
    `smsmessagetemplate`.`createdate`,
    `smsmessagetemplate`.`createuser`,
    `smsmessagetemplate`.`orgid`,
    `smsmessagetemplate`.`smsmessagetemplatebody`,
    `smsmessagetemplate`.`smsmessagetemplateid`,
    `smsmessagetemplate`.`smsmessagetemplatename`

FROM
    {{ source("bay_dbo", "smsmessagetemplate") }} AS `smsmessagetemplate`
