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
            "`comment`",
            "`createdate`",
            "`createuser`",
            "`isfollowupdate`",
            "`issendemail`",
            "`issendsms`",
            "`orgid`",
            "`scheduleenddate`",
            "`schedulestartdate`",
            "`sendcount`",
            "`senddate`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`workplacebranchid`",
            "`workplaceid`",
            "`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `comment`,
    `createdate`,
    `createuser`,
    `isfollowupdate`,
    `issendemail`,
    `issendsms`,
    `orgid`,
    `scheduleenddate`,
    `schedulestartdate`,
    `sendcount`,
    `senddate`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`,
    `workplacebranchid`,
    `workplaceid`,
    `workplacescheduleid`

FROM
    {{ source("bay_dbo", "workplaceschedule") }}
