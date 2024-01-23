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
            "refvaccine.createdate",
            "refvaccine.createuser",
            "refvaccine.description",
            "refvaccine.instruction",
            "refvaccine.orgid",
            "refvaccine.sortorder",
            "refvaccine.status",
            "refvaccine.updatedate",
            "refvaccine.updateuser",
            "refvaccine.vaccinecode",
            "refvaccine.vaccinedosegroupid",
            "refvaccine.vaccinedosenumber",
            "refvaccine.vaccinedosetype",
            "refvaccine.vaccineid",
            "refvaccine.vaccineprovidertype"
        ])
    }} AS ingestion_sk,
    refvaccine.createdate,
    refvaccine.createuser,
    refvaccine.description,
    refvaccine.instruction,
    refvaccine.orgid,
    refvaccine.sortorder,
    refvaccine.status,
    refvaccine.updatedate,
    refvaccine.updateuser,
    refvaccine.vaccinecode,
    refvaccine.vaccinedosegroupid,
    refvaccine.vaccinedosenumber,
    refvaccine.vaccinedosetype,
    refvaccine.vaccineid,
    refvaccine.vaccineprovidertype

FROM
    {{ source("bay_dbo", "refvaccine") }} AS refvaccine
