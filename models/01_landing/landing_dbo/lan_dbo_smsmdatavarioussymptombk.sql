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
            "smsmdatavarioussymptombk.collectiondate",
            "smsmdatavarioussymptombk.createdate",
            "smsmdatavarioussymptombk.ispulled",
            "smsmdatavarioussymptombk.mdatatype",
            "smsmdatavarioussymptombk.orgid",
            "smsmdatavarioussymptombk.patientid",
            "smsmdatavarioussymptombk.smsmdvarioussymptombkid",
            "smsmdatavarioussymptombk.smsmdvarioussymptomid"
        ])
    }} AS ingestion_sk,
    smsmdatavarioussymptombk.collectiondate,
    smsmdatavarioussymptombk.createdate,
    smsmdatavarioussymptombk.ispulled,
    smsmdatavarioussymptombk.mdatatype,
    smsmdatavarioussymptombk.orgid,
    smsmdatavarioussymptombk.patientid,
    smsmdatavarioussymptombk.smsmdvarioussymptombkid,
    smsmdatavarioussymptombk.smsmdvarioussymptomid

FROM
    {{ source("bay_dbo", "smsmdatavarioussymptombk") }} AS smsmdatavarioussymptombk
