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
            "`smslog`.`createdate`",
            "`smslog`.`orgid`",
            "`smslog`.`smsclassname`",
            "`smslog`.`smseventid`",
            "`smslog`.`smsexceptionmessage`",
            "`smslog`.`smsexceptionstacktrace`",
            "`smslog`.`smslogid`",
            "`smslog`.`smsmethodname`"
        ])
    }} AS `ingestion_sk`,
    `smslog`.`createdate`,
    `smslog`.`orgid`,
    `smslog`.`smsclassname`,
    `smslog`.`smseventid`,
    `smslog`.`smsexceptionmessage`,
    `smslog`.`smsexceptionstacktrace`,
    `smslog`.`smslogid`,
    `smslog`.`smsmethodname`

FROM
    {{ source("bay_dbo", "smslog") }} AS `smslog`
