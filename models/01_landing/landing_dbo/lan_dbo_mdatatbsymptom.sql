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
            "`mdtbsymptomid`",
            "`orgid`",
            "`otherssymptom`",
            "`patientid`",
            "`status`",
            "`tbsymptom`",
            "`tbsymptom1`",
            "`tbsymptom2`",
            "`tbsymptom3`",
            "`tbsymptom4`",
            "`tbsymptom5`",
            "`tbsymptom6`",
            "`tbsymptom7`",
            "`tbsymptom8`",
            "`tbsymptomcode`",
            "`tbsymptomcode1`",
            "`tbsymptomcode2`",
            "`tbsymptomcode3`",
            "`tbsymptomcode4`",
            "`tbsymptomcode5`",
            "`tbsymptomcode6`",
            "`tbsymptomcode7`",
            "`tbsymptomcode8`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdtbsymptomid`,
    `orgid`,
    `otherssymptom`,
    `patientid`,
    `status`,
    `tbsymptom`,
    `tbsymptom1`,
    `tbsymptom2`,
    `tbsymptom3`,
    `tbsymptom4`,
    `tbsymptom5`,
    `tbsymptom6`,
    `tbsymptom7`,
    `tbsymptom8`,
    `tbsymptomcode`,
    `tbsymptomcode1`,
    `tbsymptomcode2`,
    `tbsymptomcode3`,
    `tbsymptomcode4`,
    `tbsymptomcode5`,
    `tbsymptomcode6`,
    `tbsymptomcode7`,
    `tbsymptomcode8`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatatbsymptom") }}
