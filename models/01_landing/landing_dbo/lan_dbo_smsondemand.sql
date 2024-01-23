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
            "smsondemand.banglaunicode",
            "smsondemand.createdate",
            "smsondemand.createuser",
            "smsondemand.extrajson",
            "smsondemand.hasmobileno",
            "smsondemand.isbanglasmsbody",
            "smsondemand.ispulled",
            "smsondemand.isrunimmediately",
            "smsondemand.issendall",
            "smsondemand.mobileno",
            "smsondemand.orgid",
            "smsondemand.senddate",
            "smsondemand.smsbody",
            "smsondemand.smsondemandid",
            "smsondemand.smssubject",
            "smsondemand.status",
            "smsondemand.updatedate",
            "smsondemand.updateuser",
            "smsondemand.workplacebranchid",
            "smsondemand.workplaceid"
        ])
    }} AS ingestion_sk,
    smsondemand.banglaunicode,
    smsondemand.createdate,
    smsondemand.createuser,
    smsondemand.extrajson,
    smsondemand.hasmobileno,
    smsondemand.isbanglasmsbody,
    smsondemand.ispulled,
    smsondemand.isrunimmediately,
    smsondemand.issendall,
    smsondemand.mobileno,
    smsondemand.orgid,
    smsondemand.senddate,
    smsondemand.smsbody,
    smsondemand.smsondemandid,
    smsondemand.smssubject,
    smsondemand.status,
    smsondemand.updatedate,
    smsondemand.updateuser,
    smsondemand.workplacebranchid,
    smsondemand.workplaceid

FROM
    {{ source("bay_dbo", "smsondemand") }} AS smsondemand
