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
            "`attachedfilenames`",
            "`bccaddress`",
            "`ccaddress`",
            "`createdate`",
            "`createuser`",
            "`emailbody`",
            "`emaileventid`",
            "`emailid`",
            "`emailresponse`",
            "`emailsenddate`",
            "`emailsubject`",
            "`fromaddress`",
            "`isattachedfile`",
            "`issentemail`",
            "`isuploademail`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`toaddress`",
            "`updatedate`",
            "`updateuser`",
            "`workplacebranchid`",
            "`workplaceid`",
            "`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `attachedfilenames`,
    `bccaddress`,
    `ccaddress`,
    `createdate`,
    `createuser`,
    `emailbody`,
    `emaileventid`,
    `emailid`,
    `emailresponse`,
    `emailsenddate`,
    `emailsubject`,
    `fromaddress`,
    `isattachedfile`,
    `issentemail`,
    `isuploademail`,
    `orgid`,
    `patientid`,
    `status`,
    `toaddress`,
    `updatedate`,
    `updateuser`,
    `workplacebranchid`,
    `workplaceid`,
    `workplacescheduleid`

FROM
    {{ source("bay_dbo", "emaillog") }}
