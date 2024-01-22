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
            "`mdpatientpeid`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`tbepastevidencedcode`",
            "`tbepastevidencedcode1`",
            "`tbepastevidencedcode2`",
            "`tbepastevidencedcode3`",
            "`tbepastevidencedcode4`",
            "`tbepastevidencedcode5`",
            "`tbepastevidencedcode6`",
            "`tbepastevidencedcode7`",
            "`tbepastevidencedcode8`",
            "`tbepastevidencedid`",
            "`tbepastevidencedid1`",
            "`tbepastevidencedid2`",
            "`tbepastevidencedid3`",
            "`tbepastevidencedid4`",
            "`tbepastevidencedid5`",
            "`tbepastevidencedid6`",
            "`tbepastevidencedid7`",
            "`tbepastevidencedid8`",
            "`tbepastevidencedothers`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdpatientpeid`,
    `orgid`,
    `patientid`,
    `status`,
    `tbepastevidencedcode`,
    `tbepastevidencedcode1`,
    `tbepastevidencedcode2`,
    `tbepastevidencedcode3`,
    `tbepastevidencedcode4`,
    `tbepastevidencedcode5`,
    `tbepastevidencedcode6`,
    `tbepastevidencedcode7`,
    `tbepastevidencedcode8`,
    `tbepastevidencedid`,
    `tbepastevidencedid1`,
    `tbepastevidencedid2`,
    `tbepastevidencedid3`,
    `tbepastevidencedid4`,
    `tbepastevidencedid5`,
    `tbepastevidencedid6`,
    `tbepastevidencedid7`,
    `tbepastevidencedid8`,
    `tbepastevidencedothers`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatatbepastevidenced") }}
