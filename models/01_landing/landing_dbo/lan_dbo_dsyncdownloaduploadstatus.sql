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
            "`downloaduploadindicator`",
            "`dsyncdownloaduploadstatusid`",
            "`ipaddress`",
            "`isfinalupload`",
            "`operationdate`",
            "`updatedate`",
            "`updateuser`",
            "`workplaceid`",
            "`workplacescheduleid`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `downloaduploadindicator`,
    `dsyncdownloaduploadstatusid`,
    `ipaddress`,
    `isfinalupload`,
    `operationdate`,
    `updatedate`,
    `updateuser`,
    `workplaceid`,
    `workplacescheduleid`

FROM
    {{ source("bay_dbo", "dsyncdownloaduploadstatus") }}
