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
            "`answergroupid`",
            "`createdate`",
            "`createuser`",
            "`mappingquestionanswerid`",
            "`orgid`",
            "`questiongroupid`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `answergroupid`,
    `createdate`,
    `createuser`,
    `mappingquestionanswerid`,
    `orgid`,
    `questiongroupid`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mapquestionanswer") }}
