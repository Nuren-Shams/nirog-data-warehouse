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
            "createdate",
            "createuser",
            "eventid",
            "isbanglasmsbody",
            "issend",
            "mobileno",
            "orgid",
            "sendcount",
            "smsbody",
            "smsrelid",
            "smsresponse",
            "smstablebkid",
            "smstableid",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    eventid,
    isbanglasmsbody,
    issend,
    mobileno,
    orgid,
    sendcount,
    smsbody,
    smsrelid,
    smsresponse,
    smstablebkid,
    smstableid,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "smstablebk") }}
