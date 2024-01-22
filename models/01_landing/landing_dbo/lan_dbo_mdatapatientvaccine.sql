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
            "`adulttype`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`isgivenbynirog`",
            "`mdpatientvaccineid`",
            "`orgid`",
            "`othervaccine`",
            "`patientid`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`vaccineid`"
        ])
    }} AS `ingestion_sk`,
    `adulttype`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `isgivenbynirog`,
    `mdpatientvaccineid`,
    `orgid`,
    `othervaccine`,
    `patientid`,
    `status`,
    `updatedate`,
    `updateuser`,
    `vaccineid`

FROM
    {{ source("bay_dbo", "mdatapatientvaccine") }}
