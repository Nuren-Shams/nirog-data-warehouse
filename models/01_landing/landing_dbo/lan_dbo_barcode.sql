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
            "`barcode_based64`",
            "`barcodeid`",
            "`patientid`"
        ])
    }} AS `ingestion_sk`,
    `barcode_based64`,
    `barcodeid`,
    `patientid`

FROM
    {{ source("bay_dbo", "barcode") }}
