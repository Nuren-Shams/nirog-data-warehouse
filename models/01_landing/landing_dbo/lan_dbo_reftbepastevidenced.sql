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
            "`tbepastevidencecode`",
            "`tbepastevidenceid`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `description`,
    `orgid`,
    `sortorder`,
    `status`,
    `tbepastevidencecode`,
    `tbepastevidenceid`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "reftbepastevidenced") }}
