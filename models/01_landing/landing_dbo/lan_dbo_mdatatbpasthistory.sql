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
            "`mdatatbpasthistory`.`collectiondate`",
            "`mdatatbpasthistory`.`createdate`",
            "`mdatatbpasthistory`.`createuser`",
            "`mdatatbpasthistory`.`mdpatienttbphistoryid`",
            "`mdatatbpasthistory`.`orgid`",
            "`mdatatbpasthistory`.`patientid`",
            "`mdatatbpasthistory`.`status`",
            "`mdatatbpasthistory`.`tbhistoryanswer1`",
            "`mdatatbpasthistory`.`tbhistoryanswer2`",
            "`mdatatbpasthistory`.`tbhistoryanswer3`",
            "`mdatatbpasthistory`.`tbhistoryothers1`",
            "`mdatatbpasthistory`.`tbhistoryothers2`",
            "`mdatatbpasthistory`.`tbpasthistoryquestionid`",
            "`mdatatbpasthistory`.`updatedate`",
            "`mdatatbpasthistory`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mdatatbpasthistory`.`collectiondate`,
    `mdatatbpasthistory`.`createdate`,
    `mdatatbpasthistory`.`createuser`,
    `mdatatbpasthistory`.`mdpatienttbphistoryid`,
    `mdatatbpasthistory`.`orgid`,
    `mdatatbpasthistory`.`patientid`,
    `mdatatbpasthistory`.`status`,
    `mdatatbpasthistory`.`tbhistoryanswer1`,
    `mdatatbpasthistory`.`tbhistoryanswer2`,
    `mdatatbpasthistory`.`tbhistoryanswer3`,
    `mdatatbpasthistory`.`tbhistoryothers1`,
    `mdatatbpasthistory`.`tbhistoryothers2`,
    `mdatatbpasthistory`.`tbpasthistoryquestionid`,
    `mdatatbpasthistory`.`updatedate`,
    `mdatatbpasthistory`.`updateuser`

FROM
    {{ source("bay_dbo", "mdatatbpasthistory") }} AS `mdatatbpasthistory`
