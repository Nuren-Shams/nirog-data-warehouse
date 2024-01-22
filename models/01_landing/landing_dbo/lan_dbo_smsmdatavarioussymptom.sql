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
            "`collectiondate`",
            "`createdate`",
            "`ispulled`",
            "`mdatatype`",
            "`orgid`",
            "`patientid`",
            "`smsmdvarioussymptomid`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `ispulled`,
    `mdatatype`,
    `orgid`,
    `patientid`,
    `smsmdvarioussymptomid`

FROM
    {{ source("bay_dbo", "smsmdatavarioussymptom") }}
