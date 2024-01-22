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
            "fidpstatus",
            "ppstatus",
            "regid",
            "updatedate",
            "updateuserid"
        ])
    }} AS ingestion_sk,
    createdate,
    createuserid,
    fidpstatus,
    ppstatus,
    regid,
    updatedate,
    updateuserid

FROM
    {{ source("bay_dbo", "printregid") }}
