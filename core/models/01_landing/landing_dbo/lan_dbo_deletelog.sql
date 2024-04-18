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
            "`deletelog`.`inscollist`",
            "`deletelog`.`insvalues`",
            "`deletelog`.`logdate`",
            "`deletelog`.`logid`",
            "`deletelog`.`pkname`",
            "`deletelog`.`pkvalue`",
            "`deletelog`.`tabname`"
        ])
    }} AS `ingestion_sk`,
    `deletelog`.`inscollist`,
    `deletelog`.`insvalues`,
    `deletelog`.`logdate`,
    `deletelog`.`logid`,
    `deletelog`.`pkname`,
    `deletelog`.`pkvalue`,
    `deletelog`.`tabname`

FROM
    {{ source("bay_dbo", "deletelog") }} AS `deletelog`
