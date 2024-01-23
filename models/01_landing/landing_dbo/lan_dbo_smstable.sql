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
            "smstable.createdate",
            "smstable.createuser",
            "smstable.eventid",
            "smstable.isbanglasmsbody",
            "smstable.issend",
            "smstable.mobileno",
            "smstable.orgid",
            "smstable.sendcount",
            "smstable.smsbody",
            "smstable.smsrelid",
            "smstable.smsresponse",
            "smstable.smstableid",
            "smstable.updatedate",
            "smstable.updateuser"
        ])
    }} AS ingestion_sk,
    smstable.createdate,
    smstable.createuser,
    smstable.eventid,
    smstable.isbanglasmsbody,
    smstable.issend,
    smstable.mobileno,
    smstable.orgid,
    smstable.sendcount,
    smstable.smsbody,
    smstable.smsrelid,
    smstable.smsresponse,
    smstable.smstableid,
    smstable.updatedate,
    smstable.updateuser

FROM
    {{ source("bay_dbo", "smstable") }} AS smstable
