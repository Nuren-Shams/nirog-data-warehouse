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
            "`mdatapatientnewvaccine`.`actualdate`",
            "`mdatapatientnewvaccine`.`advice`",
            "`mdatapatientnewvaccine`.`collectiondate`",
            "`mdatapatientnewvaccine`.`comment`",
            "`mdatapatientnewvaccine`.`createdate`",
            "`mdatapatientnewvaccine`.`createuser`",
            "`mdatapatientnewvaccine`.`mdpatientnewvaccineid`",
            "`mdatapatientnewvaccine`.`orgid`",
            "`mdatapatientnewvaccine`.`other`",
            "`mdatapatientnewvaccine`.`patientid`",
            "`mdatapatientnewvaccine`.`status`",
            "`mdatapatientnewvaccine`.`suggesteddate`",
            "`mdatapatientnewvaccine`.`updatedate`",
            "`mdatapatientnewvaccine`.`updateuser`",
            "`mdatapatientnewvaccine`.`vaccinedoseid`",
            "`mdatapatientnewvaccine`.`vaccineid`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatientnewvaccine`.`actualdate`,
    `mdatapatientnewvaccine`.`advice`,
    `mdatapatientnewvaccine`.`collectiondate`,
    `mdatapatientnewvaccine`.`comment`,
    `mdatapatientnewvaccine`.`createdate`,
    `mdatapatientnewvaccine`.`createuser`,
    `mdatapatientnewvaccine`.`mdpatientnewvaccineid`,
    `mdatapatientnewvaccine`.`orgid`,
    `mdatapatientnewvaccine`.`other`,
    `mdatapatientnewvaccine`.`patientid`,
    `mdatapatientnewvaccine`.`status`,
    `mdatapatientnewvaccine`.`suggesteddate`,
    `mdatapatientnewvaccine`.`updatedate`,
    `mdatapatientnewvaccine`.`updateuser`,
    `mdatapatientnewvaccine`.`vaccinedoseid`,
    `mdatapatientnewvaccine`.`vaccineid`

FROM
    {{ source("bay_dbo", "mdatapatientnewvaccine") }} AS `mdatapatientnewvaccine`
