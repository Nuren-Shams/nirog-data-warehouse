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
            "hcmaphealthcenteraddress.addressid",
            "hcmaphealthcenteraddress.createdate",
            "hcmaphealthcenteraddress.createuser",
            "hcmaphealthcenteraddress.healthcenterid",
            "hcmaphealthcenteraddress.mappinghealthcenteraddressid",
            "hcmaphealthcenteraddress.orgid",
            "hcmaphealthcenteraddress.status",
            "hcmaphealthcenteraddress.updatedate",
            "hcmaphealthcenteraddress.updateuser"
        ])
    }} AS ingestion_sk,
    hcmaphealthcenteraddress.addressid,
    hcmaphealthcenteraddress.createdate,
    hcmaphealthcenteraddress.createuser,
    hcmaphealthcenteraddress.healthcenterid,
    hcmaphealthcenteraddress.mappinghealthcenteraddressid,
    hcmaphealthcenteraddress.orgid,
    hcmaphealthcenteraddress.status,
    hcmaphealthcenteraddress.updatedate,
    hcmaphealthcenteraddress.updateuser

FROM
    {{ source("bay_dbo", "hcmaphealthcenteraddress") }} AS hcmaphealthcenteraddress
