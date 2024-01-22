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
            "`createuserid`",
            "`deviceid`",
            "`devicename`",
            "`macid`",
            "`status`",
            "`updatedate`",
            "`updateuserid`",
            "`userid`",
            "`workplacebranchid`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuserid`,
    `deviceid`,
    `devicename`,
    `macid`,
    `status`,
    `updatedate`,
    `updateuserid`,
    `userid`,
    `workplacebranchid`

FROM
    {{ source("bay_dbo", "deviceregistration") }}
