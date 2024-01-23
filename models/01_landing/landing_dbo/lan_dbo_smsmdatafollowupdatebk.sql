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
            "`smsmdatafollowupdatebk`.`collectiondate`",
            "`smsmdatafollowupdatebk`.`createdate`",
            "`smsmdatafollowupdatebk`.`followupdate`",
            "`smsmdatafollowupdatebk`.`ispulled`",
            "`smsmdatafollowupdatebk`.`orgid`",
            "`smsmdatafollowupdatebk`.`patientid`",
            "`smsmdatafollowupdatebk`.`smsmdfollowupdatebkid`",
            "`smsmdatafollowupdatebk`.`smsmdfollowupdateid`"
        ])
    }} AS `ingestion_sk`,
    `smsmdatafollowupdatebk`.`collectiondate`,
    `smsmdatafollowupdatebk`.`createdate`,
    `smsmdatafollowupdatebk`.`followupdate`,
    `smsmdatafollowupdatebk`.`ispulled`,
    `smsmdatafollowupdatebk`.`orgid`,
    `smsmdatafollowupdatebk`.`patientid`,
    `smsmdatafollowupdatebk`.`smsmdfollowupdatebkid`,
    `smsmdatafollowupdatebk`.`smsmdfollowupdateid`

FROM
    {{ source("bay_dbo", "smsmdatafollowupdatebk") }} AS `smsmdatafollowupdatebk`
