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
            "`mdatapatientreferralback`.`biopsyresultid`",
            "`mdatapatientreferralback`.`bpdiastolic`",
            "`mdatapatientreferralback`.`bpsystolic`",
            "`mdatapatientreferralback`.`cancertreatmentid`",
            "`mdatapatientreferralback`.`collectiondate`",
            "`mdatapatientreferralback`.`createdate`",
            "`mdatapatientreferralback`.`createuser`",
            "`mdatapatientreferralback`.`doctorcomment`",
            "`mdatapatientreferralback`.`doctorname`",
            "`mdatapatientreferralback`.`fbg`",
            "`mdatapatientreferralback`.`healthcenterid`",
            "`mdatapatientreferralback`.`mdatapatientreferralbackid`",
            "`mdatapatientreferralback`.`orgid`",
            "`mdatapatientreferralback`.`otherfindings`",
            "`mdatapatientreferralback`.`otherpalliativecare`",
            "`mdatapatientreferralback`.`patientid`",
            "`mdatapatientreferralback`.`presenthealthstatusid`",
            "`mdatapatientreferralback`.`rbg`",
            "`mdatapatientreferralback`.`rid`",
            "`mdatapatientreferralback`.`status`",
            "`mdatapatientreferralback`.`tbsputumteststatusid`",
            "`mdatapatientreferralback`.`updatedate`",
            "`mdatapatientreferralback`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatientreferralback`.`biopsyresultid`,
    `mdatapatientreferralback`.`bpdiastolic`,
    `mdatapatientreferralback`.`bpsystolic`,
    `mdatapatientreferralback`.`cancertreatmentid`,
    `mdatapatientreferralback`.`collectiondate`,
    `mdatapatientreferralback`.`createdate`,
    `mdatapatientreferralback`.`createuser`,
    `mdatapatientreferralback`.`doctorcomment`,
    `mdatapatientreferralback`.`doctorname`,
    `mdatapatientreferralback`.`fbg`,
    `mdatapatientreferralback`.`healthcenterid`,
    `mdatapatientreferralback`.`mdatapatientreferralbackid`,
    `mdatapatientreferralback`.`orgid`,
    `mdatapatientreferralback`.`otherfindings`,
    `mdatapatientreferralback`.`otherpalliativecare`,
    `mdatapatientreferralback`.`patientid`,
    `mdatapatientreferralback`.`presenthealthstatusid`,
    `mdatapatientreferralback`.`rbg`,
    `mdatapatientreferralback`.`rid`,
    `mdatapatientreferralback`.`status`,
    `mdatapatientreferralback`.`tbsputumteststatusid`,
    `mdatapatientreferralback`.`updatedate`,
    `mdatapatientreferralback`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatapatientreferralback") }} AS `mdatapatientreferralback`
