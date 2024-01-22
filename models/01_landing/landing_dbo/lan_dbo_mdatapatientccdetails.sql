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
            "`ccdurationvalue`",
            "`ccid`",
            "`chiefcomplain`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`durationid`",
            "`mdccid`",
            "`nature`",
            "`orgid`",
            "`othercc`",
            "`patientid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `ccdurationvalue`,
    `ccid`,
    `chiefcomplain`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `durationid`,
    `mdccid`,
    `nature`,
    `orgid`,
    `othercc`,
    `patientid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatapatientccdetails") }}
