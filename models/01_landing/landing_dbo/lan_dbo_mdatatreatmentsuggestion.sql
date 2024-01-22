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
            "collectiondate",
            "comment",
            "createdate",
            "createuser",
            "drugdose",
            "drugdurationvalue",
            "drugid",
            "durationid",
            "frequency",
            "hourly",
            "mdtreatmentsuggestionid",
            "orgid",
            "otherdrug",
            "patientid",
            "reffrequencyid",
            "refinstructionid",
            "specialinstruction",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    collectiondate,
    comment,
    createdate,
    createuser,
    drugdose,
    drugdurationvalue,
    drugid,
    durationid,
    frequency,
    hourly,
    mdtreatmentsuggestionid,
    orgid,
    otherdrug,
    patientid,
    reffrequencyid,
    refinstructionid,
    specialinstruction,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatatreatmentsuggestion") }}
