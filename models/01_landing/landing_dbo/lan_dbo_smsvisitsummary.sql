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
            "smsvisitsummary.collectiondate",
            "smsvisitsummary.createdate",
            "smsvisitsummary.ispulled",
            "smsvisitsummary.orgid",
            "smsvisitsummary.patientid",
            "smsvisitsummary.smsvisitsummaryid"
        ])
    }} AS ingestion_sk,
    smsvisitsummary.collectiondate,
    smsvisitsummary.createdate,
    smsvisitsummary.ispulled,
    smsvisitsummary.orgid,
    smsvisitsummary.patientid,
    smsvisitsummary.smsvisitsummaryid

FROM
    {{ source("bay_dbo", "smsvisitsummary") }} AS smsvisitsummary
