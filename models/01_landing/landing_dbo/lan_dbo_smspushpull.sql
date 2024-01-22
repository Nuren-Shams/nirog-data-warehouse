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
            "`createdate`",
            "`createuser`",
            "`ispushonly`",
            "`isvalidformat`",
            "`mobileno`",
            "`orgid`",
            "`parentid`",
            "`smspushpullid`",
            "`smsrequest`",
            "`smsresponse`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `ispushonly`,
    `isvalidformat`,
    `mobileno`,
    `orgid`,
    `parentid`,
    `smspushpullid`,
    `smsrequest`,
    `smsresponse`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "smspushpull") }}
