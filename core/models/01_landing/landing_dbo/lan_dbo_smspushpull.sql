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
            "`smspushpull`.`createdate`",
            "`smspushpull`.`createuser`",
            "`smspushpull`.`ispushonly`",
            "`smspushpull`.`isvalidformat`",
            "`smspushpull`.`mobileno`",
            "`smspushpull`.`orgid`",
            "`smspushpull`.`smspushpullid`",
            "`smspushpull`.`smsrequest`",
            "`smspushpull`.`smsresponse`",
            "`smspushpull`.`updatedate`",
            "`smspushpull`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `smspushpull`.`createdate`,
    `smspushpull`.`createuser`,
    `smspushpull`.`ispushonly`,
    `smspushpull`.`isvalidformat`,
    `smspushpull`.`mobileno`,
    `smspushpull`.`orgid`,
    `smspushpull`.`smspushpullid`,
    `smspushpull`.`smsrequest`,
    `smspushpull`.`smsresponse`,
    `smspushpull`.`updatedate`,
    `smspushpull`.`updateuser`

FROM
    {{ source("bay_dbo", "smspushpull") }} AS `smspushpull`
