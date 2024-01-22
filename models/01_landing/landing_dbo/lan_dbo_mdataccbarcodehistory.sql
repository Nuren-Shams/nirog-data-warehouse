{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "barcodehistoryid",
            "barcodeid",
            "createdate",
            "createuser",
            "endcode",
            "startingcode",
            "status",
            "totalnumber",
            "updatedate",
            "updateuser",
            "userid"
        ])
    }} AS ingestion_sk,
    barcodehistoryid,
    barcodeid,
    createdate,
    createuser,
    endcode,
    startingcode,
    status,
    totalnumber,
    updatedate,
    updateuser,
    userid

FROM
    {{ source("bay_dbo", "mdataccbarcodehistory") }}
