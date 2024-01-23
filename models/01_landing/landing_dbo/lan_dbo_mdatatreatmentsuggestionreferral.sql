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
            "`mdatatreatmentsuggestionreferral`.`collectiondate`",
            "`mdatatreatmentsuggestionreferral`.`comment`",
            "`mdatatreatmentsuggestionreferral`.`createdate`",
            "`mdatatreatmentsuggestionreferral`.`createuser`",
            "`mdatatreatmentsuggestionreferral`.`drugdurationvalue`",
            "`mdatatreatmentsuggestionreferral`.`drugid`",
            "`mdatatreatmentsuggestionreferral`.`durationid`",
            "`mdatatreatmentsuggestionreferral`.`frequency`",
            "`mdatatreatmentsuggestionreferral`.`hourly`",
            "`mdatatreatmentsuggestionreferral`.`mdatapatientreferralbackid`",
            "`mdatatreatmentsuggestionreferral`.`mdtreatmentsuggestionreferralid`",
            "`mdatatreatmentsuggestionreferral`.`orgid`",
            "`mdatatreatmentsuggestionreferral`.`otherdrug`",
            "`mdatatreatmentsuggestionreferral`.`patientid`",
            "`mdatatreatmentsuggestionreferral`.`reffrequencyid`",
            "`mdatatreatmentsuggestionreferral`.`refinstructionid`",
            "`mdatatreatmentsuggestionreferral`.`specialinstruction`",
            "`mdatatreatmentsuggestionreferral`.`status`",
            "`mdatatreatmentsuggestionreferral`.`updatedate`",
            "`mdatatreatmentsuggestionreferral`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatatreatmentsuggestionreferral`.`collectiondate`,
    `mdatatreatmentsuggestionreferral`.`comment`,
    `mdatatreatmentsuggestionreferral`.`createdate`,
    `mdatatreatmentsuggestionreferral`.`createuser`,
    `mdatatreatmentsuggestionreferral`.`drugdurationvalue`,
    `mdatatreatmentsuggestionreferral`.`drugid`,
    `mdatatreatmentsuggestionreferral`.`durationid`,
    `mdatatreatmentsuggestionreferral`.`frequency`,
    `mdatatreatmentsuggestionreferral`.`hourly`,
    `mdatatreatmentsuggestionreferral`.`mdatapatientreferralbackid`,
    `mdatatreatmentsuggestionreferral`.`mdtreatmentsuggestionreferralid`,
    `mdatatreatmentsuggestionreferral`.`orgid`,
    `mdatatreatmentsuggestionreferral`.`otherdrug`,
    `mdatatreatmentsuggestionreferral`.`patientid`,
    `mdatatreatmentsuggestionreferral`.`reffrequencyid`,
    `mdatatreatmentsuggestionreferral`.`refinstructionid`,
    `mdatatreatmentsuggestionreferral`.`specialinstruction`,
    `mdatatreatmentsuggestionreferral`.`status`,
    `mdatatreatmentsuggestionreferral`.`updatedate`,
    `mdatatreatmentsuggestionreferral`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatatreatmentsuggestionreferral") }} AS `mdatatreatmentsuggestionreferral`
