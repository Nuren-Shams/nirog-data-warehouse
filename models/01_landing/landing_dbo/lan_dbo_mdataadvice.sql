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
            "mdataadvice.advice",
            "mdataadvice.adviceid",
            "mdataadvice.collectiondate",
            "mdataadvice.createdate",
            "mdataadvice.createuser",
            "mdataadvice.mdadviceid",
            "mdataadvice.orgid",
            "mdataadvice.patientid",
            "mdataadvice.status",
            "mdataadvice.updatedate",
            "mdataadvice.updateuser"
        ])
    }} AS ingestion_sk,
    mdataadvice.advice,
    mdataadvice.adviceid,
    mdataadvice.collectiondate,
    mdataadvice.createdate,
    mdataadvice.createuser,
    mdataadvice.mdadviceid,
    mdataadvice.orgid,
    mdataadvice.patientid,
    mdataadvice.status,
    mdataadvice.updatedate,
    mdataadvice.updateuser

FROM
    {{ source("bay_dbo", "mdataadvice") }} AS mdataadvice
