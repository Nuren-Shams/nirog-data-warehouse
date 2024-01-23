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
            "mdatafinding.collectiondate",
            "mdatafinding.createdate",
            "mdatafinding.createuser",
            "mdatafinding.finding",
            "mdatafinding.mdfindingid",
            "mdatafinding.orgid",
            "mdatafinding.patientid",
            "mdatafinding.status",
            "mdatafinding.updatedate",
            "mdatafinding.updateuser"
        ])
    }} AS ingestion_sk,
    mdatafinding.collectiondate,
    mdatafinding.createdate,
    mdatafinding.createuser,
    mdatafinding.finding,
    mdatafinding.mdfindingid,
    mdatafinding.orgid,
    mdatafinding.patientid,
    mdatafinding.status,
    mdatafinding.updatedate,
    mdatafinding.updateuser

FROM
    {{ source("bay_dbo", "mdatafinding") }} AS mdatafinding
