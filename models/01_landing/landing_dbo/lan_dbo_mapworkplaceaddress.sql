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
            "addressid",
            "createdate",
            "createuser",
            "mappingworkplaceaddressid",
            "orgid",
            "status",
            "updatedate",
            "updateuser",
            "workplaceid"
        ])
    }} AS ingestion_sk,
    addressid,
    createdate,
    createuser,
    mappingworkplaceaddressid,
    orgid,
    status,
    updatedate,
    updateuser,
    workplaceid

FROM
    {{ source("bay_dbo", "mapworkplaceaddress") }}
