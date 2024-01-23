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
            "refdruggroup.createdate",
            "refdruggroup.createuser",
            "refdruggroup.description",
            "refdruggroup.druggroupcode",
            "refdruggroup.druggroupid",
            "refdruggroup.orgid",
            "refdruggroup.sortorder",
            "refdruggroup.status",
            "refdruggroup.updatedate",
            "refdruggroup.updateuser"
        ])
    }} AS ingestion_sk,
    refdruggroup.createdate,
    refdruggroup.createuser,
    refdruggroup.description,
    refdruggroup.druggroupcode,
    refdruggroup.druggroupid,
    refdruggroup.orgid,
    refdruggroup.sortorder,
    refdruggroup.status,
    refdruggroup.updatedate,
    refdruggroup.updateuser

FROM
    {{ source("bay_dbo", "refdruggroup") }} AS refdruggroup
