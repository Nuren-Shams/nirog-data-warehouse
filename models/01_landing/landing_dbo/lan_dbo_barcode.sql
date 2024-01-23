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
            "barcode.barcode_based64",
            "barcode.barcodeid",
            "barcode.patientid"
        ])
    }} AS ingestion_sk,
    barcode.barcode_based64,
    barcode.barcodeid,
    barcode.patientid

FROM
    {{ source("bay_dbo", "barcode") }} AS barcode
