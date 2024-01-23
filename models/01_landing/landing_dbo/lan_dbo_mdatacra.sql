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
            "mdatacra.age",
            "mdatacra.bmi",
            "mdatacra.cigarettesmoker",
            "mdatacra.cratype",
            "mdatacra.createdate",
            "mdatacra.createuser",
            "mdatacra.diabetese",
            "mdatacra.hdlcholesterol",
            "mdatacra.mdatacraid",
            "mdatacra.onbloodpressuremedication",
            "mdatacra.orgid",
            "mdatacra.patientid",
            "mdatacra.result",
            "mdatacra.sex",
            "mdatacra.status",
            "mdatacra.systolicbloodpressure",
            "mdatacra.totalcholesterol",
            "mdatacra.updatedate",
            "mdatacra.updateuser"
        ])
    }} AS ingestion_sk,
    mdatacra.age,
    mdatacra.bmi,
    mdatacra.cigarettesmoker,
    mdatacra.cratype,
    mdatacra.createdate,
    mdatacra.createuser,
    mdatacra.diabetese,
    mdatacra.hdlcholesterol,
    mdatacra.mdatacraid,
    mdatacra.onbloodpressuremedication,
    mdatacra.orgid,
    mdatacra.patientid,
    mdatacra.result,
    mdatacra.sex,
    mdatacra.status,
    mdatacra.systolicbloodpressure,
    mdatacra.totalcholesterol,
    mdatacra.updatedate,
    mdatacra.updateuser

FROM
    {{ source("bay_dbo", "mdatacra") }} AS mdatacra
