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
            "mdataglucosehb.collectiondate",
            "mdataglucosehb.createdate",
            "mdataglucosehb.createuser",
            "mdataglucosehb.fbg",
            "mdataglucosehb.hemoglobin",
            "mdataglucosehb.hrsfromlasteat",
            "mdataglucosehb.id",
            "mdataglucosehb.orgid",
            "mdataglucosehb.patientid",
            "mdataglucosehb.rbg",
            "mdataglucosehb.status",
            "mdataglucosehb.updatedate",
            "mdataglucosehb.updateuser"
        ])
    }} AS ingestion_sk,
    mdataglucosehb.collectiondate,
    mdataglucosehb.createdate,
    mdataglucosehb.createuser,
    mdataglucosehb.fbg,
    mdataglucosehb.hemoglobin,
    mdataglucosehb.hrsfromlasteat,
    mdataglucosehb.id,
    mdataglucosehb.orgid,
    mdataglucosehb.patientid,
    mdataglucosehb.rbg,
    mdataglucosehb.status,
    mdataglucosehb.updatedate,
    mdataglucosehb.updateuser

FROM
    {{ source("bay_dbo", "mdataglucosehb") }} AS mdataglucosehb
