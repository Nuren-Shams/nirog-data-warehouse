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
            "mdataccbarcodehistory.barcodehistoryid",
            "mdataccbarcodehistory.barcodeid",
            "mdataccbarcodehistory.createdate",
            "mdataccbarcodehistory.createuser",
            "mdataccbarcodehistory.endcode",
            "mdataccbarcodehistory.startingcode",
            "mdataccbarcodehistory.status",
            "mdataccbarcodehistory.totalnumber",
            "mdataccbarcodehistory.updatedate",
            "mdataccbarcodehistory.updateuser",
            "mdataccbarcodehistory.userid"
        ])
    }} AS ingestion_sk,
    mdataccbarcodehistory.barcodehistoryid,
    mdataccbarcodehistory.barcodeid,
    mdataccbarcodehistory.createdate,
    mdataccbarcodehistory.createuser,
    mdataccbarcodehistory.endcode,
    mdataccbarcodehistory.startingcode,
    mdataccbarcodehistory.status,
    mdataccbarcodehistory.totalnumber,
    mdataccbarcodehistory.updatedate,
    mdataccbarcodehistory.updateuser,
    mdataccbarcodehistory.userid

FROM
    {{ source("bay_dbo", "mdataccbarcodehistory") }} AS mdataccbarcodehistory
