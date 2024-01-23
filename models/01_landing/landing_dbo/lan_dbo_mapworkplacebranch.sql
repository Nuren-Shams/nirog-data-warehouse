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
            "mapworkplacebranch.createdate",
            "mapworkplacebranch.createuser",
            "mapworkplacebranch.mappingworkplacebranch",
            "mapworkplacebranch.orgid",
            "mapworkplacebranch.status",
            "mapworkplacebranch.updatedate",
            "mapworkplacebranch.updateuser",
            "mapworkplacebranch.workplacebranchid",
            "mapworkplacebranch.workplaceid"
        ])
    }} AS ingestion_sk,
    mapworkplacebranch.createdate,
    mapworkplacebranch.createuser,
    mapworkplacebranch.mappingworkplacebranch,
    mapworkplacebranch.orgid,
    mapworkplacebranch.status,
    mapworkplacebranch.updatedate,
    mapworkplacebranch.updateuser,
    mapworkplacebranch.workplacebranchid,
    mapworkplacebranch.workplaceid

FROM
    {{ source("bay_dbo", "mapworkplacebranch") }} AS mapworkplacebranch
