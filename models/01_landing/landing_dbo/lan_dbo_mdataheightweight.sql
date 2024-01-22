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
            "`bmi`",
            "`bmistatus`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`height`",
            "`id`",
            "`muac`",
            "`muacstatus`",
            "`orgid`",
            "`patientid`",
            "`refbloodgroupid`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`weight`"
        ])
    }} AS `ingestion_sk`,
    `bmi`,
    `bmistatus`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `height`,
    `id`,
    `muac`,
    `muacstatus`,
    `orgid`,
    `patientid`,
    `refbloodgroupid`,
    `status`,
    `updatedate`,
    `updateuser`,
    `weight`

FROM
    {{ source("bay_dbo", "mdataheightweight") }}
