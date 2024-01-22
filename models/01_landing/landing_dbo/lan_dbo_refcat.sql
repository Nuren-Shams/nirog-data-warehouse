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
            "`catid`",
            "`catmedicinestatus`",
            "`cattype`",
            "`collectiondate`",
            "`comment1`",
            "`comment2`",
            "`createdate`",
            "`createuser`",
            "`drugdose`",
            "`drugdurationvalue`",
            "`drugid`",
            "`frequency`",
            "`hour`",
            "`orgid`",
            "`otherdrug`",
            "`specialinstruction`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `catid`,
    `catmedicinestatus`,
    `cattype`,
    `collectiondate`,
    `comment1`,
    `comment2`,
    `createdate`,
    `createuser`,
    `drugdose`,
    `drugdurationvalue`,
    `drugid`,
    `frequency`,
    `hour`,
    `orgid`,
    `otherdrug`,
    `specialinstruction`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "refcat") }}
