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
            "`mdatasocialbehavior`.`collectiondate`",
            "`mdatasocialbehavior`.`createdate`",
            "`mdatasocialbehavior`.`createuser`",
            "`mdatasocialbehavior`.`mdsocialbehaviorid`",
            "`mdatasocialbehavior`.`orgid`",
            "`mdatasocialbehavior`.`othersocialbehavior`",
            "`mdatasocialbehavior`.`patientid`",
            "`mdatasocialbehavior`.`socialbehaviorid`",
            "`mdatasocialbehavior`.`status`",
            "`mdatasocialbehavior`.`updatedate`",
            "`mdatasocialbehavior`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatasocialbehavior`.`collectiondate`,
    `mdatasocialbehavior`.`createdate`,
    `mdatasocialbehavior`.`createuser`,
    `mdatasocialbehavior`.`mdsocialbehaviorid`,
    `mdatasocialbehavior`.`orgid`,
    `mdatasocialbehavior`.`othersocialbehavior`,
    `mdatasocialbehavior`.`patientid`,
    `mdatasocialbehavior`.`socialbehaviorid`,
    `mdatasocialbehavior`.`status`,
    `mdatasocialbehavior`.`updatedate`,
    `mdatasocialbehavior`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatasocialbehavior") }} AS `mdatasocialbehavior`
