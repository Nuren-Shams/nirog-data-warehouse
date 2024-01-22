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
            "`orgid`",
            "`smstoken`",
            "`smstokenid`",
            "`smstokenlength`"
        ])
    }} AS `ingestion_sk`,
    `orgid`,
    `smstoken`,
    `smstokenid`,
    `smstokenlength`

FROM
    {{ source("bay_dbo", "smstoken") }}
