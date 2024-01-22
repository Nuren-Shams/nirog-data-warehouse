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
            "addressline1",
            "addressline1parmanent",
            "addressline2",
            "addressline2parmanent",
            "blocknumber",
            "camp",
            "country",
            "countryparmanent",
            "createdate",
            "createuser",
            "district",
            "districtparmanent",
            "fcn",
            "majhi",
            "orgid",
            "patientid",
            "postcode",
            "postcodeparmanent",
            "status",
            "tentnumber",
            "thana",
            "thanaparmanent",
            "unionid",
            "unionidparmanent",
            "updatedate",
            "updateuser",
            "village",
            "villageparmanent"
        ])
    }} AS ingestion_sk,
    addressid,
    addressline1,
    addressline1parmanent,
    addressline2,
    addressline2parmanent,
    blocknumber,
    camp,
    country,
    countryparmanent,
    createdate,
    createuser,
    district,
    districtparmanent,
    fcn,
    majhi,
    orgid,
    patientid,
    postcode,
    postcodeparmanent,
    status,
    tentnumber,
    thana,
    thanaparmanent,
    unionid,
    unionidparmanent,
    updatedate,
    updateuser,
    village,
    villageparmanent

FROM
    {{ source("bay_dbo", "address") }}
