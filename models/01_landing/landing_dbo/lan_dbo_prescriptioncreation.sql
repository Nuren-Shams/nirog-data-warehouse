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
            "`employeeid`",
            "`orgid`",
            "`patientid`",
            "`prescriptioncreationid`",
            "`prescriptionid`",
            "`status`",
            "`updatedate`",
            "`updateuser`"
        ])
    }} AS `ingestion_sk`,
    `createdate`,
    `createuser`,
    `employeeid`,
    `orgid`,
    `patientid`,
    `prescriptioncreationid`,
    `prescriptionid`,
    `status`,
    `updatedate`,
    `updateuser`

FROM
    {{ source("bay_dbo", "prescriptioncreation") }}
