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
            "mapemployeeaddress.addressid",
            "mapemployeeaddress.createdate",
            "mapemployeeaddress.createuser",
            "mapemployeeaddress.employeeid",
            "mapemployeeaddress.mappingemployeeaddressid",
            "mapemployeeaddress.orgid",
            "mapemployeeaddress.status",
            "mapemployeeaddress.updatedate",
            "mapemployeeaddress.updateuser"
        ])
    }} AS ingestion_sk,
    mapemployeeaddress.addressid,
    mapemployeeaddress.createdate,
    mapemployeeaddress.createuser,
    mapemployeeaddress.employeeid,
    mapemployeeaddress.mappingemployeeaddressid,
    mapemployeeaddress.orgid,
    mapemployeeaddress.status,
    mapemployeeaddress.updatedate,
    mapemployeeaddress.updateuser

FROM
    {{ source("bay_dbo", "mapemployeeaddress") }} AS mapemployeeaddress
