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
            "`mdatatbepastevidenced`.`collectiondate`",
            "`mdatatbepastevidenced`.`createdate`",
            "`mdatatbepastevidenced`.`createuser`",
            "`mdatatbepastevidenced`.`mdpatientpeid`",
            "`mdatatbepastevidenced`.`orgid`",
            "`mdatatbepastevidenced`.`patientid`",
            "`mdatatbepastevidenced`.`status`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode1`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode2`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode3`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode4`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode5`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode6`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode7`",
            "`mdatatbepastevidenced`.`tbepastevidencedcode8`",
            "`mdatatbepastevidenced`.`tbepastevidencedid`",
            "`mdatatbepastevidenced`.`tbepastevidencedid1`",
            "`mdatatbepastevidenced`.`tbepastevidencedid2`",
            "`mdatatbepastevidenced`.`tbepastevidencedid3`",
            "`mdatatbepastevidenced`.`tbepastevidencedid4`",
            "`mdatatbepastevidenced`.`tbepastevidencedid5`",
            "`mdatatbepastevidenced`.`tbepastevidencedid6`",
            "`mdatatbepastevidenced`.`tbepastevidencedid7`",
            "`mdatatbepastevidenced`.`tbepastevidencedid8`",
            "`mdatatbepastevidenced`.`tbepastevidencedothers`",
            "`mdatatbepastevidenced`.`updatedate`",
            "`mdatatbepastevidenced`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatatbepastevidenced`.`collectiondate`,
    `mdatatbepastevidenced`.`createdate`,
    `mdatatbepastevidenced`.`createuser`,
    `mdatatbepastevidenced`.`mdpatientpeid`,
    `mdatatbepastevidenced`.`orgid`,
    `mdatatbepastevidenced`.`patientid`,
    `mdatatbepastevidenced`.`status`,
    `mdatatbepastevidenced`.`tbepastevidencedcode`,
    `mdatatbepastevidenced`.`tbepastevidencedcode1`,
    `mdatatbepastevidenced`.`tbepastevidencedcode2`,
    `mdatatbepastevidenced`.`tbepastevidencedcode3`,
    `mdatatbepastevidenced`.`tbepastevidencedcode4`,
    `mdatatbepastevidenced`.`tbepastevidencedcode5`,
    `mdatatbepastevidenced`.`tbepastevidencedcode6`,
    `mdatatbepastevidenced`.`tbepastevidencedcode7`,
    `mdatatbepastevidenced`.`tbepastevidencedcode8`,
    `mdatatbepastevidenced`.`tbepastevidencedid`,
    `mdatatbepastevidenced`.`tbepastevidencedid1`,
    `mdatatbepastevidenced`.`tbepastevidencedid2`,
    `mdatatbepastevidenced`.`tbepastevidencedid3`,
    `mdatatbepastevidenced`.`tbepastevidencedid4`,
    `mdatatbepastevidenced`.`tbepastevidencedid5`,
    `mdatatbepastevidenced`.`tbepastevidencedid6`,
    `mdatatbepastevidenced`.`tbepastevidencedid7`,
    `mdatatbepastevidenced`.`tbepastevidencedid8`,
    `mdatatbepastevidenced`.`tbepastevidencedothers`,
    `mdatatbepastevidenced`.`updatedate`,
    `mdatatbepastevidenced`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatatbepastevidenced") }} AS `mdatatbepastevidenced`
