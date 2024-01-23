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
            "`refadvice`.`advicecode`",
            "`refadvice`.`adviceid`",
            "`refadvice`.`adviceinbangla`",
            "`refadvice`.`adviceinenglish`",
            "`refadvice`.`createdate`",
            "`refadvice`.`createuser`",
            "`refadvice`.`description`",
            "`refadvice`.`orgid`",
            "`refadvice`.`sortorder`",
            "`refadvice`.`status`",
            "`refadvice`.`updatedate`",
            "`refadvice`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `refadvice`.`advicecode`,
    `refadvice`.`adviceid`,
    `refadvice`.`adviceinbangla`,
    `refadvice`.`adviceinenglish`,
    `refadvice`.`createdate`,
    `refadvice`.`createuser`,
    `refadvice`.`description`,
    `refadvice`.`orgid`,
    `refadvice`.`sortorder`,
    `refadvice`.`status`,
    `refadvice`.`updatedate`,
    `refadvice`.`updateuser`

FROM
    {{ source("bay_dbo", "refadvice") }} AS `refadvice`
