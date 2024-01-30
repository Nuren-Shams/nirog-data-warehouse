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
            "`mdatafamilyillnesshistory`.`collectiondate`",
            "`mdatafamilyillnesshistory`.`createdate`",
            "`mdatafamilyillnesshistory`.`createuser`",
            "`mdatafamilyillnesshistory`.`deceasedyears`",
            "`mdatafamilyillnesshistory`.`illfamilymemberid`",
            "`mdatafamilyillnesshistory`.`illnessid`",
            "`mdatafamilyillnesshistory`.`mdfamilyillnessid`",
            "`mdatafamilyillnesshistory`.`orgid`",
            "`mdatafamilyillnesshistory`.`otherillfamilymember`",
            "`mdatafamilyillnesshistory`.`otherillness`",
            "`mdatafamilyillnesshistory`.`patientid`",
            "`mdatafamilyillnesshistory`.`status`",
            "`mdatafamilyillnesshistory`.`updatedate`",
            "`mdatafamilyillnesshistory`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatafamilyillnesshistory`.`collectiondate`,
    `mdatafamilyillnesshistory`.`createdate`,
    `mdatafamilyillnesshistory`.`createuser`,
    `mdatafamilyillnesshistory`.`deceasedyears`,
    `mdatafamilyillnesshistory`.`illfamilymemberid`,
    `mdatafamilyillnesshistory`.`illnessid`,
    `mdatafamilyillnesshistory`.`mdfamilyillnessid`,
    `mdatafamilyillnesshistory`.`orgid`,
    `mdatafamilyillnesshistory`.`otherillfamilymember`,
    `mdatafamilyillnesshistory`.`otherillness`,
    `mdatafamilyillnesshistory`.`patientid`,
    `mdatafamilyillnesshistory`.`status`,
    `mdatafamilyillnesshistory`.`updatedate`,
    `mdatafamilyillnesshistory`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatafamilyillnesshistory") }} AS `mdatafamilyillnesshistory`
