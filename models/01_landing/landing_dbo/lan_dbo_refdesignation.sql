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
            "`designationtitle`",
            "`orgid`",
            "`refdepartmentid`",
            "`refdesignationid`",
            "`sortorder`",
            "`status`",
            "`updatedate`",
            "`updateuser`",
            "`workplaceid`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `description`,
    `designationtitle`,
    `orgid`,
    `refdepartmentid`,
    `refdesignationid`,
    `sortorder`,
    `status`,
    `updatedate`,
    `updateuser`,
    `workplaceid`

FROM
    {{ source("bay_dbo", "refdesignation") }}
