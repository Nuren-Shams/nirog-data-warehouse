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
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`mdpatienttbphistoryid`",
            "`orgid`",
            "`patientid`",
            "`status`",
            "`tbhistoryanswer1`",
            "`tbhistoryanswer2`",
            "`tbhistoryanswer3`",
            "`tbhistoryothers1`",
            "`tbhistoryothers2`",
            "`tbpasthistoryquestionid`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdpatienttbphistoryid`,
    `orgid`,
    `patientid`,
    `status`,
    `tbhistoryanswer1`,
    `tbhistoryanswer2`,
    `tbhistoryanswer3`,
    `tbhistoryothers1`,
    `tbhistoryothers2`,
    `tbpasthistoryquestionid`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatatbpasthistory") }}
