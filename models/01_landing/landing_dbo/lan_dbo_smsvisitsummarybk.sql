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
            "smsvisitsummarybk.collectiondate",
            "smsvisitsummarybk.createdate",
            "smsvisitsummarybk.ispulled",
            "smsvisitsummarybk.orgid",
            "smsvisitsummarybk.patientid",
            "smsvisitsummarybk.smsvisitsummarybkid",
            "smsvisitsummarybk.smsvisitsummaryid"
        ])
    }} AS ingestion_sk,
    smsvisitsummarybk.collectiondate,
    smsvisitsummarybk.createdate,
    smsvisitsummarybk.ispulled,
    smsvisitsummarybk.orgid,
    smsvisitsummarybk.patientid,
    smsvisitsummarybk.smsvisitsummarybkid,
    smsvisitsummarybk.smsvisitsummaryid

FROM
    {{ source("bay_dbo", "smsvisitsummarybk") }} AS smsvisitsummarybk
