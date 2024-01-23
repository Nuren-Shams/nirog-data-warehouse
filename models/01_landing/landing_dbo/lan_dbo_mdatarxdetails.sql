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
            "mdatarxdetails.allergytomedication",
            "mdatarxdetails.collectiondate",
            "mdatarxdetails.createdate",
            "mdatarxdetails.createuser",
            "mdatarxdetails.dose",
            "mdatarxdetails.drugid",
            "mdatarxdetails.durationid",
            "mdatarxdetails.frequencyhour",
            "mdatarxdetails.orgid",
            "mdatarxdetails.patientid",
            "mdatarxdetails.rx",
            "mdatarxdetails.rxdurationvalue",
            "mdatarxdetails.rxid",
            "mdatarxdetails.status",
            "mdatarxdetails.updatedate",
            "mdatarxdetails.updateuser"
        ])
    }} AS ingestion_sk,
    mdatarxdetails.allergytomedication,
    mdatarxdetails.collectiondate,
    mdatarxdetails.createdate,
    mdatarxdetails.createuser,
    mdatarxdetails.dose,
    mdatarxdetails.drugid,
    mdatarxdetails.durationid,
    mdatarxdetails.frequencyhour,
    mdatarxdetails.orgid,
    mdatarxdetails.patientid,
    mdatarxdetails.rx,
    mdatarxdetails.rxdurationvalue,
    mdatarxdetails.rxid,
    mdatarxdetails.status,
    mdatarxdetails.updatedate,
    mdatarxdetails.updateuser

FROM
    {{ source("bay_dbo", "mdatarxdetails") }} AS mdatarxdetails
