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
            "createdate",
            "createuser",
            "description",
            "instruction",
            "orgid",
            "sortorder",
            "status",
            "updatedate",
            "updateuser",
            "vaccinecode",
            "vaccinedosegroupid",
            "vaccinedosenumber",
            "vaccinedosetype",
            "vaccineid",
            "vaccineprovidertype"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    description,
    instruction,
    orgid,
    sortorder,
    status,
    updatedate,
    updateuser,
    vaccinecode,
    vaccinedosegroupid,
    vaccinedosenumber,
    vaccinedosetype,
    vaccineid,
    vaccineprovidertype

FROM
    {{ source("bay_dbo", "refvaccineadult") }}
