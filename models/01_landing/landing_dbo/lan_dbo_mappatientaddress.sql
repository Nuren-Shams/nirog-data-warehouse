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
            "mappatientaddress.addressid",
            "mappatientaddress.createdate",
            "mappatientaddress.createuser",
            "mappatientaddress.mappingpatientaddressid",
            "mappatientaddress.orgid",
            "mappatientaddress.patientid",
            "mappatientaddress.status",
            "mappatientaddress.updatedate",
            "mappatientaddress.updateuser"
        ])
    }} AS ingestion_sk,
    mappatientaddress.addressid,
    mappatientaddress.createdate,
    mappatientaddress.createuser,
    mappatientaddress.mappingpatientaddressid,
    mappatientaddress.orgid,
    mappatientaddress.patientid,
    mappatientaddress.status,
    mappatientaddress.updatedate,
    mappatientaddress.updateuser

FROM
    {{ source("bay_dbo", "mappatientaddress") }} AS mappatientaddress
