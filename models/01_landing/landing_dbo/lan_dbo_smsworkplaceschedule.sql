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
            "`isfollowupdate`",
            "`ispulled`",
            "`ispulledforfollowup`",
            "`issendsms`",
            "`orgid`",
            "`scheduleenddate`",
            "`schedulestartdate`",
            "`smsworkplacescheduleid`",
            "`status`",
            "`workplacebranchid`",
            "`workplaceid`",
            "`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `isfollowupdate`,
    `ispulled`,
    `ispulledforfollowup`,
    `issendsms`,
    `orgid`,
    `scheduleenddate`,
    `schedulestartdate`,
    `smsworkplacescheduleid`,
    `status`,
    `workplacebranchid`,
    `workplaceid`,
    `workplacescheduleid`

FROM
    {{ source("bay_dbo", "smsworkplaceschedule") }}
