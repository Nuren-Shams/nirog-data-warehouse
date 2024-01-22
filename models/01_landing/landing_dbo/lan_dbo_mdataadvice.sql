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
            "`advice`",
            "`adviceid`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`mdadviceid`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `advice`,
    `adviceid`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdadviceid`,
    `orgid`,
    `patientid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdataadvice") }}
