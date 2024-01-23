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
            "mdataphysicalexamgeneral.anemiaseverity",
            "mdataphysicalexamgeneral.collectiondate",
            "mdataphysicalexamgeneral.createdate",
            "mdataphysicalexamgeneral.createuser",
            "mdataphysicalexamgeneral.cyanosis",
            "mdataphysicalexamgeneral.edemaseverity",
            "mdataphysicalexamgeneral.heartwithnad",
            "mdataphysicalexamgeneral.isheartwithnad",
            "mdataphysicalexamgeneral.islungswithnad",
            "mdataphysicalexamgeneral.islymphnodeswithpalpable",
            "mdataphysicalexamgeneral.jaundiceseverity",
            "mdataphysicalexamgeneral.lungswithnad",
            "mdataphysicalexamgeneral.lymphnodeswithpalpable",
            "mdataphysicalexamgeneral.lymphnodeswithpalpablesite",
            "mdataphysicalexamgeneral.lymphnodeswithpalpablesize",
            "mdataphysicalexamgeneral.mdphysicalexamgeneralid",
            "mdataphysicalexamgeneral.orgid",
            "mdataphysicalexamgeneral.othersymptom",
            "mdataphysicalexamgeneral.patientid",
            "mdataphysicalexamgeneral.status",
            "mdataphysicalexamgeneral.updatedate",
            "mdataphysicalexamgeneral.updateuser"
        ])
    }} AS ingestion_sk,
    mdataphysicalexamgeneral.anemiaseverity,
    mdataphysicalexamgeneral.collectiondate,
    mdataphysicalexamgeneral.createdate,
    mdataphysicalexamgeneral.createuser,
    mdataphysicalexamgeneral.cyanosis,
    mdataphysicalexamgeneral.edemaseverity,
    mdataphysicalexamgeneral.heartwithnad,
    mdataphysicalexamgeneral.isheartwithnad,
    mdataphysicalexamgeneral.islungswithnad,
    mdataphysicalexamgeneral.islymphnodeswithpalpable,
    mdataphysicalexamgeneral.jaundiceseverity,
    mdataphysicalexamgeneral.lungswithnad,
    mdataphysicalexamgeneral.lymphnodeswithpalpable,
    mdataphysicalexamgeneral.lymphnodeswithpalpablesite,
    mdataphysicalexamgeneral.lymphnodeswithpalpablesize,
    mdataphysicalexamgeneral.mdphysicalexamgeneralid,
    mdataphysicalexamgeneral.orgid,
    mdataphysicalexamgeneral.othersymptom,
    mdataphysicalexamgeneral.patientid,
    mdataphysicalexamgeneral.status,
    mdataphysicalexamgeneral.updatedate,
    mdataphysicalexamgeneral.updateuser

FROM
    {{ source("bay_dbo", "mdataphysicalexamgeneral") }} AS mdataphysicalexamgeneral
