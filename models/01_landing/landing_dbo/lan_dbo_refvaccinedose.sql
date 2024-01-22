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
            "`description`",
            "`orgid`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`vaccinedosegroupid`",
            "`vaccinedoseid`",
            "`vaccinedosetitle`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `description`,
    `orgid`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`,
    `vaccinedosegroupid`,
    `vaccinedoseid`,
    `vaccinedosetitle`

FROM
    {{ source("bay_dbo", "refvaccinedose") }}
