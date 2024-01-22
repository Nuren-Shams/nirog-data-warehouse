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
            "`collectiondate`",
            "`createdate`",
            "`createuser`",
            "`instruction`",
            "`investigationid`",
            "`mdinvestigationid`",
            "`orgid`",
            "`otherinvestigation`",
            "`patientid`",
            "`positivenegativestatus`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `collectiondate`,
    `createdate`,
    `createuser`,
    `instruction`,
    `investigationid`,
    `mdinvestigationid`,
    `orgid`,
    `otherinvestigation`,
    `patientid`,
    `positivenegativestatus`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "mdatainvestigation") }}
