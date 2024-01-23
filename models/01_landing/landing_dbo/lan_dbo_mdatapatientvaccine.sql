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
            "`mdatapatientvaccine`.`adulttype`",
            "`mdatapatientvaccine`.`collectiondate`",
            "`mdatapatientvaccine`.`createdate`",
            "`mdatapatientvaccine`.`createuser`",
            "`mdatapatientvaccine`.`isgivenbynirog`",
            "`mdatapatientvaccine`.`mdpatientvaccineid`",
            "`mdatapatientvaccine`.`orgid`",
            "`mdatapatientvaccine`.`othervaccine`",
            "`mdatapatientvaccine`.`patientid`",
            "`mdatapatientvaccine`.`status`",
            "`mdatapatientvaccine`.`updatedate`",
            "`mdatapatientvaccine`.`updateuser`",
            "`mdatapatientvaccine`.`vaccineid`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatientvaccine`.`adulttype`,
    `mdatapatientvaccine`.`collectiondate`,
    `mdatapatientvaccine`.`createdate`,
    `mdatapatientvaccine`.`createuser`,
    `mdatapatientvaccine`.`isgivenbynirog`,
    `mdatapatientvaccine`.`mdpatientvaccineid`,
    `mdatapatientvaccine`.`orgid`,
    `mdatapatientvaccine`.`othervaccine`,
    `mdatapatientvaccine`.`patientid`,
    `mdatapatientvaccine`.`status`,
    `mdatapatientvaccine`.`updatedate`,
    `mdatapatientvaccine`.`updateuser`,
    `mdatapatientvaccine`.`vaccineid`

FROM
    {{ source("bay_dbo", "mdatapatientvaccine") }} AS `mdatapatientvaccine`
