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
            "regid.createdate",
            "regid.createuserid",
            "regid.regid",
            "regid.status",
            "regid.updatedate",
            "regid.updateuserid",
            "regid.workplacebranchid"
        ])
    }} AS ingestion_sk,
    regid.createdate,
    regid.createuserid,
    regid.regid,
    regid.status,
    regid.updatedate,
    regid.updateuserid,
    regid.workplacebranchid

FROM
    {{ source("bay_dbo", "regid") }} AS regid
