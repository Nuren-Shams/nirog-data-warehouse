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
            "hcinpatientservice.createdate",
            "hcinpatientservice.createuser",
            "hcinpatientservice.generalinpaientfee",
            "hcinpatientservice.hcinpatientserviceid",
            "hcinpatientservice.healthcenterid",
            "hcinpatientservice.hospitalbeds",
            "hcinpatientservice.inpatientadmissionhoursendtime",
            "hcinpatientservice.inpatientadmissionhoursstarttime",
            "hcinpatientservice.isprovideinpatientservice",
            "hcinpatientservice.orgid",
            "hcinpatientservice.status",
            "hcinpatientservice.updatedate",
            "hcinpatientservice.updateuser"
        ])
    }} AS ingestion_sk,
    hcinpatientservice.createdate,
    hcinpatientservice.createuser,
    hcinpatientservice.generalinpaientfee,
    hcinpatientservice.hcinpatientserviceid,
    hcinpatientservice.healthcenterid,
    hcinpatientservice.hospitalbeds,
    hcinpatientservice.inpatientadmissionhoursendtime,
    hcinpatientservice.inpatientadmissionhoursstarttime,
    hcinpatientservice.isprovideinpatientservice,
    hcinpatientservice.orgid,
    hcinpatientservice.status,
    hcinpatientservice.updatedate,
    hcinpatientservice.updateuser

FROM
    {{ source("bay_dbo", "hcinpatientservice") }} AS hcinpatientservice
