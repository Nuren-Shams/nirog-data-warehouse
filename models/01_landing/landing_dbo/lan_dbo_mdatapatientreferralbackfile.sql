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
            "`mdatapatientreferralbackfile`.`collectiondate`",
            "`mdatapatientreferralbackfile`.`comment`",
            "`mdatapatientreferralbackfile`.`contenttype`",
            "`mdatapatientreferralbackfile`.`createdate`",
            "`mdatapatientreferralbackfile`.`createuser`",
            "`mdatapatientreferralbackfile`.`filedata`",
            "`mdatapatientreferralbackfile`.`filelocation`",
            "`mdatapatientreferralbackfile`.`filename`",
            "`mdatapatientreferralbackfile`.`filesize`",
            "`mdatapatientreferralbackfile`.`filetypeid`",
            "`mdatapatientreferralbackfile`.`fileuploaddate`",
            "`mdatapatientreferralbackfile`.`isfileinanotherlocation`",
            "`mdatapatientreferralbackfile`.`mdatapatientreferralbackfileid`",
            "`mdatapatientreferralbackfile`.`mdatapatientreferralbackid`",
            "`mdatapatientreferralbackfile`.`orgid`",
            "`mdatapatientreferralbackfile`.`patientid`",
            "`mdatapatientreferralbackfile`.`sortorder`",
            "`mdatapatientreferralbackfile`.`status`",
            "`mdatapatientreferralbackfile`.`updatedate`",
            "`mdatapatientreferralbackfile`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatapatientreferralbackfile`.`collectiondate`,
    `mdatapatientreferralbackfile`.`comment`,
    `mdatapatientreferralbackfile`.`contenttype`,
    `mdatapatientreferralbackfile`.`createdate`,
    `mdatapatientreferralbackfile`.`createuser`,
    `mdatapatientreferralbackfile`.`filedata`,
    `mdatapatientreferralbackfile`.`filelocation`,
    `mdatapatientreferralbackfile`.`filename`,
    `mdatapatientreferralbackfile`.`filesize`,
    `mdatapatientreferralbackfile`.`filetypeid`,
    `mdatapatientreferralbackfile`.`fileuploaddate`,
    `mdatapatientreferralbackfile`.`isfileinanotherlocation`,
    `mdatapatientreferralbackfile`.`mdatapatientreferralbackfileid`,
    `mdatapatientreferralbackfile`.`mdatapatientreferralbackid`,
    `mdatapatientreferralbackfile`.`orgid`,
    `mdatapatientreferralbackfile`.`patientid`,
    `mdatapatientreferralbackfile`.`sortorder`,
    `mdatapatientreferralbackfile`.`status`,
    `mdatapatientreferralbackfile`.`updatedate`,
    `mdatapatientreferralbackfile`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatapatientreferralbackfile") }} AS `mdatapatientreferralbackfile`
