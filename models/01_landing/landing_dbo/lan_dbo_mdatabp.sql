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
            "`bpdiastolic1`",
            "`bpdiastolic2`",
            "`bpsystolic1`",
            "`bpsystolic2`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`currenttemparature`",
            "`heartrate`",
            "`id`",
            "`indicatesnormaloxygensaturation`",
            "`orgid`",
            "`patientid`",
            "`respiratoryrate`",
            "`spo2rate`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `bpdiastolic1`,
    `bpdiastolic2`,
    `bpsystolic1`,
    `bpsystolic2`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `currenttemparature`,
    `heartrate`,
    `id`,
    `indicatesnormaloxygensaturation`,
    `orgid`,
    `patientid`,
    `respiratoryrate`,
    `spo2rate`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatabp") }}
