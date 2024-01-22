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
            "`address1`",
            "`address2`",
            "`city`",
            "`country`",
            "`createdate`",
            "`createuser`",
            "`emailaddress`",
            "`orgcode`",
            "`orgid`",
            "`orgname`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `address1`,
    `address2`,
    `city`,
    `country`,
    `createdate`,
    `createuser`,
    `emailaddress`,
    `orgcode`,
    `orgid`,
    `orgname`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "organization") }}
