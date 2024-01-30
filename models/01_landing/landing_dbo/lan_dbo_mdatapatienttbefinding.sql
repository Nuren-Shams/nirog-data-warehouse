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
            "`mdatapatienttbefinding`.`collectiondate`",
            "`mdatapatienttbefinding`.`createdate`",
            "`mdatapatienttbefinding`.`createuser`",
            "`mdatapatienttbefinding`.`mdefindingid`",
            "`mdatapatienttbefinding`.`orgid`",
            "`mdatapatienttbefinding`.`patientid`",
            "`mdatapatienttbefinding`.`status`",
            "`mdatapatienttbefinding`.`tbefindingcode`",
            "`mdatapatienttbefinding`.`tbefindingcode1`",
            "`mdatapatienttbefinding`.`tbefindingcode2`",
            "`mdatapatienttbefinding`.`tbefindingcode3`",
            "`mdatapatienttbefinding`.`tbefindingcode4`",
            "`mdatapatienttbefinding`.`tbefindingcode5`",
            "`mdatapatienttbefinding`.`tbefindingcode6`",
            "`mdatapatienttbefinding`.`tbefindingcode7`",
            "`mdatapatienttbefinding`.`tbefindingcode8`",
            "`mdatapatienttbefinding`.`tbefindingid`",
            "`mdatapatienttbefinding`.`tbefindingid1`",
            "`mdatapatienttbefinding`.`tbefindingid2`",
            "`mdatapatienttbefinding`.`tbefindingid3`",
            "`mdatapatienttbefinding`.`tbefindingid4`",
            "`mdatapatienttbefinding`.`tbefindingid5`",
            "`mdatapatienttbefinding`.`tbefindingid6`",
            "`mdatapatienttbefinding`.`tbefindingid7`",
            "`mdatapatienttbefinding`.`tbefindingid8`",
            "`mdatapatienttbefinding`.`tbefindingothers`",
            "`mdatapatienttbefinding`.`updatedate`",
            "`mdatapatienttbefinding`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatienttbefinding`.`collectiondate`,
    `mdatapatienttbefinding`.`createdate`,
    `mdatapatienttbefinding`.`createuser`,
    `mdatapatienttbefinding`.`mdefindingid`,
    `mdatapatienttbefinding`.`orgid`,
    `mdatapatienttbefinding`.`patientid`,
    `mdatapatienttbefinding`.`status`,
    `mdatapatienttbefinding`.`tbefindingcode`,
    `mdatapatienttbefinding`.`tbefindingcode1`,
    `mdatapatienttbefinding`.`tbefindingcode2`,
    `mdatapatienttbefinding`.`tbefindingcode3`,
    `mdatapatienttbefinding`.`tbefindingcode4`,
    `mdatapatienttbefinding`.`tbefindingcode5`,
    `mdatapatienttbefinding`.`tbefindingcode6`,
    `mdatapatienttbefinding`.`tbefindingcode7`,
    `mdatapatienttbefinding`.`tbefindingcode8`,
    `mdatapatienttbefinding`.`tbefindingid`,
    `mdatapatienttbefinding`.`tbefindingid1`,
    `mdatapatienttbefinding`.`tbefindingid2`,
    `mdatapatienttbefinding`.`tbefindingid3`,
    `mdatapatienttbefinding`.`tbefindingid4`,
    `mdatapatienttbefinding`.`tbefindingid5`,
    `mdatapatienttbefinding`.`tbefindingid6`,
    `mdatapatienttbefinding`.`tbefindingid7`,
    `mdatapatienttbefinding`.`tbefindingid8`,
    `mdatapatienttbefinding`.`tbefindingothers`,
    `mdatapatienttbefinding`.`updatedate`,
    `mdatapatienttbefinding`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatapatienttbefinding") }} AS `mdatapatienttbefinding`
