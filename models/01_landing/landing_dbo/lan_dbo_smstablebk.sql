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
            "smstablebk.createdate",
            "smstablebk.createuser",
            "smstablebk.eventid",
            "smstablebk.isbanglasmsbody",
            "smstablebk.issend",
            "smstablebk.mobileno",
            "smstablebk.orgid",
            "smstablebk.sendcount",
            "smstablebk.smsbody",
            "smstablebk.smsrelid",
            "smstablebk.smsresponse",
            "smstablebk.smstablebkid",
            "smstablebk.smstableid",
            "smstablebk.updatedate",
            "smstablebk.updateuser"
        ])
    }} AS ingestion_sk,
    smstablebk.createdate,
    smstablebk.createuser,
    smstablebk.eventid,
    smstablebk.isbanglasmsbody,
    smstablebk.issend,
    smstablebk.mobileno,
    smstablebk.orgid,
    smstablebk.sendcount,
    smstablebk.smsbody,
    smstablebk.smsrelid,
    smstablebk.smsresponse,
    smstablebk.smstablebkid,
    smstablebk.smstableid,
    smstablebk.updatedate,
    smstablebk.updateuser

FROM
    {{ source("bay_dbo", "smstablebk") }} AS smstablebk
