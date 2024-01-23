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
            "reftbefinding.createdate",
            "reftbefinding.createuser",
            "reftbefinding.description",
            "reftbefinding.orgid",
            "reftbefinding.sortorder",
            "reftbefinding.status",
            "reftbefinding.tbefindingcode",
            "reftbefinding.tbefindingid",
            "reftbefinding.updatedate",
            "reftbefinding.updateuser"
        ])
    }} AS ingestion_sk,
    reftbefinding.createdate,
    reftbefinding.createuser,
    reftbefinding.description,
    reftbefinding.orgid,
    reftbefinding.sortorder,
    reftbefinding.status,
    reftbefinding.tbefindingcode,
    reftbefinding.tbefindingid,
    reftbefinding.updatedate,
    reftbefinding.updateuser

FROM
    {{ source("bay_dbo", "reftbefinding") }} AS reftbefinding
