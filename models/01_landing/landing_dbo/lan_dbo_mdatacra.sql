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
            "age",
            "bmi",
            "cigarettesmoker",
            "cratype",
            "createdate",
            "createuser",
            "diabetese",
            "hdlcholesterol",
            "mdatacraid",
            "onbloodpressuremedication",
            "orgid",
            "patientid",
            "result",
            "sex",
            "status",
            "systolicbloodpressure",
            "totalcholesterol",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    age,
    bmi,
    cigarettesmoker,
    cratype,
    createdate,
    createuser,
    diabetese,
    hdlcholesterol,
    mdatacraid,
    onbloodpressuremedication,
    orgid,
    patientid,
    result,
    sex,
    status,
    systolicbloodpressure,
    totalcholesterol,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatacra") }}
