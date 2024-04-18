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
            "`smspushpullbk`.`createdate`",
            "`smspushpullbk`.`createuser`",
            "`smspushpullbk`.`ispushonly`",
            "`smspushpullbk`.`isvalidformat`",
            "`smspushpullbk`.`mobileno`",
            "`smspushpullbk`.`orgid`",
            "`smspushpullbk`.`smspushpullbkid`",
            "`smspushpullbk`.`smspushpullid`",
            "`smspushpullbk`.`smsrequest`",
            "`smspushpullbk`.`smsresponse`",
            "`smspushpullbk`.`updatedate`",
            "`smspushpullbk`.`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `smspushpullbk`.`createdate`,
    `smspushpullbk`.`createuser`,
    `smspushpullbk`.`ispushonly`,
    `smspushpullbk`.`isvalidformat`,
    `smspushpullbk`.`mobileno`,
    `smspushpullbk`.`orgid`,
    `smspushpullbk`.`smspushpullbkid`,
    `smspushpullbk`.`smspushpullid`,
    `smspushpullbk`.`smsrequest`,
    `smspushpullbk`.`smsresponse`,
    `smspushpullbk`.`updatedate`,
    `smspushpullbk`.`updateuser`

FROM
    {{ source("bay_dbo", "smspushpullbk") }} AS `smspushpullbk`
