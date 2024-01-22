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
            "`answerid`",
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`mdtbpatientquestionanswerid`",
            "`orgid`",
            "`otheranswer`",
            "`patientid`",
            "`questionid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `answerid`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `mdtbpatientquestionanswerid`,
    `orgid`,
    `otheranswer`,
    `patientid`,
    `questionid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatatbpatientquestionanswer") }}
