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
            "`smsmdatafollowupdate`.`collectiondate`",
            "`smsmdatafollowupdate`.`createdate`",
            "`smsmdatafollowupdate`.`followupdate`",
            "`smsmdatafollowupdate`.`ispulled`",
            "`smsmdatafollowupdate`.`orgid`",
            "`smsmdatafollowupdate`.`patientid`",
            "`smsmdatafollowupdate`.`smsmdfollowupdateid`"
        ])
    }} AS `ingestion_sk`,
    `smsmdatafollowupdate`.`collectiondate`,
    `smsmdatafollowupdate`.`createdate`,
    `smsmdatafollowupdate`.`followupdate`,
    `smsmdatafollowupdate`.`ispulled`,
    `smsmdatafollowupdate`.`orgid`,
    `smsmdatafollowupdate`.`patientid`,
    `smsmdatafollowupdate`.`smsmdfollowupdateid`

FROM
    {{ source("bay_dbo", "smsmdatafollowupdate") }} AS `smsmdatafollowupdate`
