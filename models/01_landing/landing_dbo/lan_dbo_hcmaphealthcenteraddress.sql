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
            "healthcenterid",
            "mappinghealthcenteraddressid",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    addressid,
    createdate,
    createuser,
    healthcenterid,
    mappinghealthcenteraddressid,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenteraddress") }}
