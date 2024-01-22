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
            "`mddate`",
            "`mdlccid`",
            "`mdstartd`",
            "`mduid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `mddate`,
    `mdlccid`,
    `mdstartd`,
    `mduid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatapatientcc") }}
