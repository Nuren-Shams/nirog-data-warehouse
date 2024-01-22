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
            "collectiondate",
            "createdate",
            "ispulled",
            "orgid",
            "patientid",
            "smsvisitsummarybkid",
            "smsvisitsummaryid"
        ])
    }} AS ingestion_sk,
    collectiondate,
    createdate,
    ispulled,
    orgid,
    patientid,
    smsvisitsummarybkid,
    smsvisitsummaryid

FROM
    {{ source("bay_dbo", "smsvisitsummarybk") }}
