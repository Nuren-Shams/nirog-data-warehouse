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
            "mdatatreatmentsuggestion.collectiondate",
            "mdatatreatmentsuggestion.comment",
            "mdatatreatmentsuggestion.createdate",
            "mdatatreatmentsuggestion.createuser",
            "mdatatreatmentsuggestion.drugdose",
            "mdatatreatmentsuggestion.drugdurationvalue",
            "mdatatreatmentsuggestion.drugid",
            "mdatatreatmentsuggestion.durationid",
            "mdatatreatmentsuggestion.frequency",
            "mdatatreatmentsuggestion.hourly",
            "mdatatreatmentsuggestion.mdtreatmentsuggestionid",
            "mdatatreatmentsuggestion.orgid",
            "mdatatreatmentsuggestion.otherdrug",
            "mdatatreatmentsuggestion.patientid",
            "mdatatreatmentsuggestion.reffrequencyid",
            "mdatatreatmentsuggestion.refinstructionid",
            "mdatatreatmentsuggestion.specialinstruction",
            "mdatatreatmentsuggestion.status",
            "mdatatreatmentsuggestion.updatedate",
            "mdatatreatmentsuggestion.updateuser"
        ])
    }} AS ingestion_sk,
    mdatatreatmentsuggestion.collectiondate,
    mdatatreatmentsuggestion.comment,
    mdatatreatmentsuggestion.createdate,
    mdatatreatmentsuggestion.createuser,
    mdatatreatmentsuggestion.drugdose,
    mdatatreatmentsuggestion.drugdurationvalue,
    mdatatreatmentsuggestion.drugid,
    mdatatreatmentsuggestion.durationid,
    mdatatreatmentsuggestion.frequency,
    mdatatreatmentsuggestion.hourly,
    mdatatreatmentsuggestion.mdtreatmentsuggestionid,
    mdatatreatmentsuggestion.orgid,
    mdatatreatmentsuggestion.otherdrug,
    mdatatreatmentsuggestion.patientid,
    mdatatreatmentsuggestion.reffrequencyid,
    mdatatreatmentsuggestion.refinstructionid,
    mdatatreatmentsuggestion.specialinstruction,
    mdatatreatmentsuggestion.status,
    mdatatreatmentsuggestion.updatedate,
    mdatatreatmentsuggestion.updateuser

FROM
    {{ source("bay_dbo", "mdatatreatmentsuggestion") }} AS mdatatreatmentsuggestion
