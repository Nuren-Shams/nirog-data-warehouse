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
            "createuserid",
            "regid",
            "status",
            "updatedate",
            "updateuserid",
            "workplacebranchid"
        ])
    }} AS ingestion_sk,
    createdate,
    createuserid,
    regid,
    status,
    updatedate,
    updateuserid,
    workplacebranchid

FROM
    {{ source("bay_dbo", "regid") }}
