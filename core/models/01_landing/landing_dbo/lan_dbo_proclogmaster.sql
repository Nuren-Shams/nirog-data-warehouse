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
            "`proclogmaster`.`logdate`",
            "`proclogmaster`.`logmessage`",
            "`proclogmaster`.`logtier`",
            "`proclogmaster`.`proclogid`",
            "`proclogmaster`.`procname`",
            "`proclogmaster`.`procseq`",
            "`proclogmaster`.`userid`",
            "`proclogmaster`.`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `proclogmaster`.`logdate`,
    `proclogmaster`.`logmessage`,
    `proclogmaster`.`logtier`,
    `proclogmaster`.`proclogid`,
    `proclogmaster`.`procname`,
    `proclogmaster`.`procseq`,
    `proclogmaster`.`userid`,
    `proclogmaster`.`workplaceid`

FROM
    {{ source("bay_dbo", "proclogmaster") }} AS `proclogmaster`
