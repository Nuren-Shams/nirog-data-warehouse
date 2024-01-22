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
            "`createdate`",
            "`createuser`",
            "`orgid`",
            "`smsmessagetemplatebody`",
            "`smsmessagetemplateid`",
            "`smsmessagetemplatename`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `orgid`,
    `smsmessagetemplatebody`,
    `smsmessagetemplateid`,
    `smsmessagetemplatename`

FROM
    {{ source("bay_dbo", "smsmessagetemplate") }}
