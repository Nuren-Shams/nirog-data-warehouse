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
            "address.addressid",
            "address.addressline1",
            "address.addressline1parmanent",
            "address.addressline2",
            "address.addressline2parmanent",
            "address.blocknumber",
            "address.camp",
            "address.country",
            "address.countryparmanent",
            "address.createdate",
            "address.createuser",
            "address.district",
            "address.districtparmanent",
            "address.fcn",
            "address.majhi",
            "address.orgid",
            "address.patientid",
            "address.postcode",
            "address.postcodeparmanent",
            "address.status",
            "address.tentnumber",
            "address.thana",
            "address.thanaparmanent",
            "address.unionid",
            "address.unionidparmanent",
            "address.updatedate",
            "address.updateuser",
            "address.village",
            "address.villageparmanent"
        ])
    }} AS ingestion_sk,
    address.addressid,
    address.addressline1,
    address.addressline1parmanent,
    address.addressline2,
    address.addressline2parmanent,
    address.blocknumber,
    address.camp,
    address.country,
    address.countryparmanent,
    address.createdate,
    address.createuser,
    address.district,
    address.districtparmanent,
    address.fcn,
    address.majhi,
    address.orgid,
    address.patientid,
    address.postcode,
    address.postcodeparmanent,
    address.status,
    address.tentnumber,
    address.thana,
    address.thanaparmanent,
    address.unionid,
    address.unionidparmanent,
    address.updatedate,
    address.updateuser,
    address.village,
    address.villageparmanent

FROM
    {{ source("bay_dbo", "address") }} AS address
