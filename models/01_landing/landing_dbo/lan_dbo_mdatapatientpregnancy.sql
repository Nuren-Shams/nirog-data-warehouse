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
            "`mdatapatientpregnancy`.`collectiondate`",
            "`mdatapatientpregnancy`.`comment`",
            "`mdatapatientpregnancy`.`createdate`",
            "`mdatapatientpregnancy`.`createuser`",
            "`mdatapatientpregnancy`.`edd`",
            "`mdatapatientpregnancy`.`foetalheartrate`",
            "`mdatapatientpregnancy`.`foetalheartsound`",
            "`mdatapatientpregnancy`.`foetalmovement`",
            "`mdatapatientpregnancy`.`foetalposition`",
            "`mdatapatientpregnancy`.`lmp`",
            "`mdatapatientpregnancy`.`mdpatientpregnancyid`",
            "`mdatapatientpregnancy`.`orgid`",
            "`mdatapatientpregnancy`.`otherfoetalpositioninfo`",
            "`mdatapatientpregnancy`.`patientid`",
            "`mdatapatientpregnancy`.`pregnancydurationinweeks`",
            "`mdatapatientpregnancy`.`status`",
            "`mdatapatientpregnancy`.`symphysiofundalheight`",
            "`mdatapatientpregnancy`.`updatedate`",
            "`mdatapatientpregnancy`.`updateuser`",
            "`mdatapatientpregnancy`.`usg`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatientpregnancy`.`collectiondate`,
    `mdatapatientpregnancy`.`comment`,
    `mdatapatientpregnancy`.`createdate`,
    `mdatapatientpregnancy`.`createuser`,
    `mdatapatientpregnancy`.`edd`,
    `mdatapatientpregnancy`.`foetalheartrate`,
    `mdatapatientpregnancy`.`foetalheartsound`,
    `mdatapatientpregnancy`.`foetalmovement`,
    `mdatapatientpregnancy`.`foetalposition`,
    `mdatapatientpregnancy`.`lmp`,
    `mdatapatientpregnancy`.`mdpatientpregnancyid`,
    `mdatapatientpregnancy`.`orgid`,
    `mdatapatientpregnancy`.`otherfoetalpositioninfo`,
    `mdatapatientpregnancy`.`patientid`,
    `mdatapatientpregnancy`.`pregnancydurationinweeks`,
    `mdatapatientpregnancy`.`status`,
    `mdatapatientpregnancy`.`symphysiofundalheight`,
    `mdatapatientpregnancy`.`updatedate`,
    `mdatapatientpregnancy`.`updateuser`,
    `mdatapatientpregnancy`.`usg`

FROM
    {{ source("bay_dbo", "mdatapatientpregnancy") }} AS `mdatapatientpregnancy`
