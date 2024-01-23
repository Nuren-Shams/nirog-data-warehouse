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
            "mapworkplacebranchaddress.addressid",
            "mapworkplacebranchaddress.createdate",
            "mapworkplacebranchaddress.createuser",
            "mapworkplacebranchaddress.mappingworkplacebranchaddressid",
            "mapworkplacebranchaddress.orgid",
            "mapworkplacebranchaddress.status",
            "mapworkplacebranchaddress.updatedate",
            "mapworkplacebranchaddress.updateuser",
            "mapworkplacebranchaddress.workplacebranchid"
        ])
    }} AS ingestion_sk,
    mapworkplacebranchaddress.addressid,
    mapworkplacebranchaddress.createdate,
    mapworkplacebranchaddress.createuser,
    mapworkplacebranchaddress.mappingworkplacebranchaddressid,
    mapworkplacebranchaddress.orgid,
    mapworkplacebranchaddress.status,
    mapworkplacebranchaddress.updatedate,
    mapworkplacebranchaddress.updateuser,
    mapworkplacebranchaddress.workplacebranchid

FROM
    {{ source("bay_dbo", "mapworkplacebranchaddress") }} AS mapworkplacebranchaddress
