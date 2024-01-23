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
            "mdatavarioussymptom.anemiaseverity",
            "mdatavarioussymptom.anemiaseverityid",
            "mdatavarioussymptom.collectiondate",
            "mdatavarioussymptom.coughgreaterthanmonth",
            "mdatavarioussymptom.createdate",
            "mdatavarioussymptom.createuser",
            "mdatavarioussymptom.lgerf",
            "mdatavarioussymptom.mdvarioussymptomid",
            "mdatavarioussymptom.nightsweat",
            "mdatavarioussymptom.orgid",
            "mdatavarioussymptom.patientid",
            "mdatavarioussymptom.status",
            "mdatavarioussymptom.updatedate",
            "mdatavarioussymptom.updateuser",
            "mdatavarioussymptom.weightloss"
        ])
    }} AS ingestion_sk,
    mdatavarioussymptom.anemiaseverity,
    mdatavarioussymptom.anemiaseverityid,
    mdatavarioussymptom.collectiondate,
    mdatavarioussymptom.coughgreaterthanmonth,
    mdatavarioussymptom.createdate,
    mdatavarioussymptom.createuser,
    mdatavarioussymptom.lgerf,
    mdatavarioussymptom.mdvarioussymptomid,
    mdatavarioussymptom.nightsweat,
    mdatavarioussymptom.orgid,
    mdatavarioussymptom.patientid,
    mdatavarioussymptom.status,
    mdatavarioussymptom.updatedate,
    mdatavarioussymptom.updateuser,
    mdatavarioussymptom.weightloss

FROM
    {{ source("bay_dbo", "mdatavarioussymptom") }} AS mdatavarioussymptom
