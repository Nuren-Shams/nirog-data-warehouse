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
            "`category`",
            "`commonterm`",
            "`createdate`",
            "`createuser`",
            "`orgid`",
            "`refprovisionaldiagnosisgroupcode`",
            "`refprovisionaldiagnosisgroupid`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `category`,
    `commonterm`,
    `createdate`,
    `createuser`,
    `orgid`,
    `refprovisionaldiagnosisgroupcode`,
    `refprovisionaldiagnosisgroupid`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "refprovisionaldiagnosisgroup") }}
