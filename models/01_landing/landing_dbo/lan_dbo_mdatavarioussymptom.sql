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
            "`anemiaseverity`",
            "`anemiaseverityid`",
            "`collectiondate`",
            "`coughgreaterthanmonth`",
            "`createdate`",
            "`createuser`",
            "`lgerf`",
            "`mdvarioussymptomid`",
            "`nightsweat`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`weightloss`"
        ])
    }} AS `ingestion_sk`,
    `anemiaseverity`,
    `anemiaseverityid`,
    `collectiondate`,
    `coughgreaterthanmonth`,
    `createdate`,
    `createuser`,
    `lgerf`,
    `mdvarioussymptomid`,
    `nightsweat`,
    `orgid`,
    `patientid`,
    `status`,
    `updatedate`,
    `updateuser`,
    `weightloss`

FROM
    {{ source("bay_dbo", "mdatavarioussymptom") }}
