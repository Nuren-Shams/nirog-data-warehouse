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
            "mdatapatientcc.createdate",
            "mdatapatientcc.createuser",
            "mdatapatientcc.mddate",
            "mdatapatientcc.mdlccid",
            "mdatapatientcc.mdstartd",
            "mdatapatientcc.mduid",
            "mdatapatientcc.status",
            "mdatapatientcc.updatedate",
            "mdatapatientcc.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientcc.createdate,
    mdatapatientcc.createuser,
    mdatapatientcc.mddate,
    mdatapatientcc.mdlccid,
    mdatapatientcc.mdstartd,
    mdatapatientcc.mduid,
    mdatapatientcc.status,
    mdatapatientcc.updatedate,
    mdatapatientcc.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientcc") }} AS mdatapatientcc
