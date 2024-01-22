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
            "`cccode`",
            "`ccid`",
            "`createdate`",
            "`createuser`",
            "`description`",
            "`orgid`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `cccode`,
    `ccid`,
    `createdate`,
    `createuser`,
    `description`,
    `orgid`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "refchiefcomplain") }}
