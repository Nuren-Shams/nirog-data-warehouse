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
            "`mdatafollowupdate`.`collectiondate`",
            "`mdatafollowupdate`.`comment`",
            "`mdatafollowupdate`.`createdate`",
            "`mdatafollowupdate`.`createuser`",
            "`mdatafollowupdate`.`followupdate`",
            "`mdatafollowupdate`.`followupindicator`",
            "`mdatafollowupdate`.`mdfollowupdateid`",
            "`mdatafollowupdate`.`orgid`",
            "`mdatafollowupdate`.`patientid`",
            "`mdatafollowupdate`.`status`",
            "`mdatafollowupdate`.`updatedate`",
            "`mdatafollowupdate`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatafollowupdate`.`collectiondate`,
    `mdatafollowupdate`.`comment`,
    `mdatafollowupdate`.`createdate`,
    `mdatafollowupdate`.`createuser`,
    `mdatafollowupdate`.`followupdate`,
    `mdatafollowupdate`.`followupindicator`,
    `mdatafollowupdate`.`mdfollowupdateid`,
    `mdatafollowupdate`.`orgid`,
    `mdatafollowupdate`.`patientid`,
    `mdatafollowupdate`.`status`,
    `mdatafollowupdate`.`updatedate`,
    `mdatafollowupdate`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatafollowupdate") }} AS `mdatafollowupdate`
