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
            "smstoken.orgid",
            "smstoken.smstoken",
            "smstoken.smstokenid",
            "smstoken.smstokenlength"
        ])
    }} AS ingestion_sk,
    smstoken.orgid,
    smstoken.smstoken,
    smstoken.smstokenid,
    smstoken.smstokenlength

FROM
    {{ source("bay_dbo", "smstoken") }} AS smstoken
