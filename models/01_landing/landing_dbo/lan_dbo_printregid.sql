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
            "`printregid`.`createdate`",
            "`printregid`.`createuserid`",
            "`printregid`.`fidpstatus`",
            "`printregid`.`ppstatus`",
            "`printregid`.`regid`",
            "`printregid`.`updatedate`",
            "`printregid`.`updateuserid`"
        ])
    }} AS `ingestion_sk`,
    `printregid`.`createdate`,
    `printregid`.`createuserid`,
    `printregid`.`fidpstatus`,
    `printregid`.`ppstatus`,
    `printregid`.`regid`,
    `printregid`.`updatedate`,
    `printregid`.`updateuserid`

FROM
    {{ source("bay_dbo", "printregid") }} AS `printregid`
