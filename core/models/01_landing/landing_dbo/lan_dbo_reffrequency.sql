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
            "`reffrequency`.`createdate`",
            "`reffrequency`.`createuser`",
            "`reffrequency`.`description`",
            "`reffrequency`.`frequencycode`",
            "`reffrequency`.`frequencyid`",
            "`reffrequency`.`frequencyinbangla`",
            "`reffrequency`.`frequencyinenglish`",
            "`reffrequency`.`orgid`",
            "`reffrequency`.`sortorder`",
            "`reffrequency`.`status`",
            "`reffrequency`.`updatedate`",
            "`reffrequency`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `reffrequency`.`createdate`,
    `reffrequency`.`createuser`,
    `reffrequency`.`description`,
    `reffrequency`.`frequencycode`,
    `reffrequency`.`frequencyid`,
    `reffrequency`.`frequencyinbangla`,
    `reffrequency`.`frequencyinenglish`,
    `reffrequency`.`orgid`,
    `reffrequency`.`sortorder`,
    `reffrequency`.`status`,
    `reffrequency`.`updatedate`,
    `reffrequency`.`updateuser`

FROM
    {{ source("bay_dbo", "reffrequency") }} AS `reffrequency`
