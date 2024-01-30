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
            "`mapquestionanswer`.`answergroupid`",
            "`mapquestionanswer`.`createdate`",
            "`mapquestionanswer`.`createuser`",
            "`mapquestionanswer`.`mappingquestionanswerid`",
            "`mapquestionanswer`.`orgid`",
            "`mapquestionanswer`.`questiongroupid`",
            "`mapquestionanswer`.`sortorder`",
            "`mapquestionanswer`.`status`",
            "`mapquestionanswer`.`updatedate`",
            "`mapquestionanswer`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `mapquestionanswer`.`answergroupid`,
    `mapquestionanswer`.`createdate`,
    `mapquestionanswer`.`createuser`,
    `mapquestionanswer`.`mappingquestionanswerid`,
    `mapquestionanswer`.`orgid`,
    `mapquestionanswer`.`questiongroupid`,
    `mapquestionanswer`.`sortorder`,
    `mapquestionanswer`.`status`,
    `mapquestionanswer`.`updatedate`,
    `mapquestionanswer`.`updateuser`

FROM
    {{ source("bay_dbo", "mapquestionanswer") }} AS `mapquestionanswer`
