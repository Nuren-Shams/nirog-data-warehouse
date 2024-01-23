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
            "mdatapatientccdetails.ccdurationvalue",
            "mdatapatientccdetails.ccid",
            "mdatapatientccdetails.chiefcomplain",
            "mdatapatientccdetails.collectiondate",
            "mdatapatientccdetails.createdate",
            "mdatapatientccdetails.createuser",
            "mdatapatientccdetails.durationid",
            "mdatapatientccdetails.mdccid",
            "mdatapatientccdetails.nature",
            "mdatapatientccdetails.orgid",
            "mdatapatientccdetails.othercc",
            "mdatapatientccdetails.patientid",
            "mdatapatientccdetails.status",
            "mdatapatientccdetails.updatedate",
            "mdatapatientccdetails.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientccdetails.ccdurationvalue,
    mdatapatientccdetails.ccid,
    mdatapatientccdetails.chiefcomplain,
    mdatapatientccdetails.collectiondate,
    mdatapatientccdetails.createdate,
    mdatapatientccdetails.createuser,
    mdatapatientccdetails.durationid,
    mdatapatientccdetails.mdccid,
    mdatapatientccdetails.nature,
    mdatapatientccdetails.orgid,
    mdatapatientccdetails.othercc,
    mdatapatientccdetails.patientid,
    mdatapatientccdetails.status,
    mdatapatientccdetails.updatedate,
    mdatapatientccdetails.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientccdetails") }} AS mdatapatientccdetails
