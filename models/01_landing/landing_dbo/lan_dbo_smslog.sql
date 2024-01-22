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
            "`orgid`",
            "`smsclassname`",
            "`smseventid`",
            "`smsexceptionmessage`",
            "`smsexceptionstacktrace`",
            "`smslogid`",
            "`smsmethodname`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `orgid`,
    `smsclassname`,
    `smseventid`,
    `smsexceptionmessage`,
    `smsexceptionstacktrace`,
    `smslogid`,
    `smsmethodname`

FROM
    {{ source("bay_dbo", "smslog") }}
