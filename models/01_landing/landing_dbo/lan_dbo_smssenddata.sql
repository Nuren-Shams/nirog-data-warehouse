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
            "banglaunicode",
            "createdate",
            "createuser",
            "isbanglasmsbody",
            "issend",
            "mobileno",
            "orgid",
            "patientid",
            "smsbody",
            "smsresponse",
            "smssenddataid"
        ])
    }} AS ingestion_sk,
    banglaunicode,
    createdate,
    createuser,
    isbanglasmsbody,
    issend,
    mobileno,
    orgid,
    patientid,
    smsbody,
    smsresponse,
    smssenddataid

FROM
    {{ source("bay_dbo", "smssenddata") }}
