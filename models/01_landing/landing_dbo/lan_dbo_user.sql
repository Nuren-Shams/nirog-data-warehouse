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
            "user.createdate",
            "user.createuser",
            "user.employeeid",
            "user.loginid",
            "user.password",
            "user.resettoken",
            "user.resettokencreatedate",
            "user.status",
            "user.updatedate",
            "user.updateuser",
            "user.userid"
        ])
    }} AS ingestion_sk,
    user.createdate,
    user.createuser,
    user.employeeid,
    user.loginid,
    user.password,
    user.resettoken,
    user.resettokencreatedate,
    user.status,
    user.updatedate,
    user.updateuser,
    user.userid

FROM
    {{ source("bay_dbo", "user") }} AS user
