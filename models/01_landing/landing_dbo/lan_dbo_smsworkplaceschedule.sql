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
            "`smsworkplaceschedule`.`createdate`",
            "`smsworkplaceschedule`.`isfollowupdate`",
            "`smsworkplaceschedule`.`ispulled`",
            "`smsworkplaceschedule`.`ispulledforfollowup`",
            "`smsworkplaceschedule`.`issendsms`",
            "`smsworkplaceschedule`.`orgid`",
            "`smsworkplaceschedule`.`scheduleenddate`",
            "`smsworkplaceschedule`.`schedulestartdate`",
            "`smsworkplaceschedule`.`smsworkplacescheduleid`",
            "`smsworkplaceschedule`.`status`",
            "`smsworkplaceschedule`.`workplacebranchid`",
            "`smsworkplaceschedule`.`workplaceid`",
            "`smsworkplaceschedule`.`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `smsworkplaceschedule`.`createdate`,
    `smsworkplaceschedule`.`isfollowupdate`,
    `smsworkplaceschedule`.`ispulled`,
    `smsworkplaceschedule`.`ispulledforfollowup`,
    `smsworkplaceschedule`.`issendsms`,
    `smsworkplaceschedule`.`orgid`,
    `smsworkplaceschedule`.`scheduleenddate`,
    `smsworkplaceschedule`.`schedulestartdate`,
    `smsworkplaceschedule`.`smsworkplacescheduleid`,
    `smsworkplaceschedule`.`status`,
    `smsworkplaceschedule`.`workplacebranchid`,
    `smsworkplaceschedule`.`workplaceid`,
    `smsworkplaceschedule`.`workplacescheduleid`

FROM
    {{ source("bay_dbo", "smsworkplaceschedule") }} AS `smsworkplaceschedule`
