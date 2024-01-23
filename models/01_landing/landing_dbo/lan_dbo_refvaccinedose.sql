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
            "refvaccinedose.createdate",
            "refvaccinedose.createuser",
            "refvaccinedose.description",
            "refvaccinedose.orgid",
            "refvaccinedose.sortorder",
            "refvaccinedose.status",
            "refvaccinedose.updatedate",
            "refvaccinedose.updateuser",
            "refvaccinedose.vaccinedosegroupid",
            "refvaccinedose.vaccinedoseid",
            "refvaccinedose.vaccinedosetitle"
        ])
    }} AS ingestion_sk,
    refvaccinedose.createdate,
    refvaccinedose.createuser,
    refvaccinedose.description,
    refvaccinedose.orgid,
    refvaccinedose.sortorder,
    refvaccinedose.status,
    refvaccinedose.updatedate,
    refvaccinedose.updateuser,
    refvaccinedose.vaccinedosegroupid,
    refvaccinedose.vaccinedoseid,
    refvaccinedose.vaccinedosetitle

FROM
    {{ source("bay_dbo", "refvaccinedose") }} AS refvaccinedose
