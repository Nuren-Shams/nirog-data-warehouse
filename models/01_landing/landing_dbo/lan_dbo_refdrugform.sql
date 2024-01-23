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
            "refdrugform.createdate",
            "refdrugform.createuser",
            "refdrugform.description",
            "refdrugform.drugformcode",
            "refdrugform.drugformid",
            "refdrugform.orgid",
            "refdrugform.sortorder",
            "refdrugform.status",
            "refdrugform.updatedate",
            "refdrugform.updateuser"
        ])
    }} AS ingestion_sk,
    refdrugform.createdate,
    refdrugform.createuser,
    refdrugform.description,
    refdrugform.drugformcode,
    refdrugform.drugformid,
    refdrugform.orgid,
    refdrugform.sortorder,
    refdrugform.status,
    refdrugform.updatedate,
    refdrugform.updateuser

FROM
    {{ source("bay_dbo", "refdrugform") }} AS refdrugform
