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
            "`createuser`",
            "`mdefindingid`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`tbefindingcode`",
            "`tbefindingcode1`",
            "`tbefindingcode2`",
            "`tbefindingcode3`",
            "`tbefindingcode4`",
            "`tbefindingcode5`",
            "`tbefindingcode6`",
            "`tbefindingcode7`",
            "`tbefindingcode8`",
            "`tbefindingid`",
            "`tbefindingid1`",
            "`tbefindingid2`",
            "`tbefindingid3`",
            "`tbefindingid4`",
            "`tbefindingid5`",
            "`tbefindingid6`",
            "`tbefindingid7`",
            "`tbefindingid8`",
            "`tbefindingothers`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdefindingid`,
    `orgid`,
    `patientid`,
    `status`,
    `tbefindingcode`,
    `tbefindingcode1`,
    `tbefindingcode2`,
    `tbefindingcode3`,
    `tbefindingcode4`,
    `tbefindingcode5`,
    `tbefindingcode6`,
    `tbefindingcode7`,
    `tbefindingcode8`,
    `tbefindingid`,
    `tbefindingid1`,
    `tbefindingid2`,
    `tbefindingid3`,
    `tbefindingid4`,
    `tbefindingid5`,
    `tbefindingid6`,
    `tbefindingid7`,
    `tbefindingid8`,
    `tbefindingothers`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatapatienttbefinding") }}
