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
            "`createuser`",
            "`emaileventid`",
            "`emailmapworkplacescheduleid`",
            "`emailsenddate`",
            "`orgid`",
            "`status`",
            "`totalsentemail`",
            "`totaluploademail`",
            "`totalvisited`",
            "`updatedate`",
            "`updateuser`",
            "`workplacebranchid`",
            "`workplaceid`",
            "`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `emaileventid`,
    `emailmapworkplacescheduleid`,
    `emailsenddate`,
    `orgid`,
    `status`,
    `totalsentemail`,
    `totaluploademail`,
    `totalvisited`,
    `updatedate`,
    `updateuser`,
    `workplacebranchid`,
    `workplaceid`,
    `workplacescheduleid`

FROM
    {{ source("bay_dbo", "emailmapworkplaceschedule") }}
