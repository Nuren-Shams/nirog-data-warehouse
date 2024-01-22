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
            "`emaileventid`",
            "`emaileventname`",
            "`emailtemplateid`",
            "`orgid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `emaileventid`,
    `emaileventname`,
    `emailtemplateid`,
    `orgid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "emailevent") }}
