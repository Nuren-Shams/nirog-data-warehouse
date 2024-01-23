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
            "mdatadiagnosticsuggestion.collectiondate",
            "mdatadiagnosticsuggestion.createdate",
            "mdatadiagnosticsuggestion.createuser",
            "mdatadiagnosticsuggestion.diagnosticsuggestion",
            "mdatadiagnosticsuggestion.mddiagnosticsuggestionid",
            "mdatadiagnosticsuggestion.orgid",
            "mdatadiagnosticsuggestion.patientid",
            "mdatadiagnosticsuggestion.status",
            "mdatadiagnosticsuggestion.updatedate",
            "mdatadiagnosticsuggestion.updateuser"
        ])
    }} AS ingestion_sk,
    mdatadiagnosticsuggestion.collectiondate,
    mdatadiagnosticsuggestion.createdate,
    mdatadiagnosticsuggestion.createuser,
    mdatadiagnosticsuggestion.diagnosticsuggestion,
    mdatadiagnosticsuggestion.mddiagnosticsuggestionid,
    mdatadiagnosticsuggestion.orgid,
    mdatadiagnosticsuggestion.patientid,
    mdatadiagnosticsuggestion.status,
    mdatadiagnosticsuggestion.updatedate,
    mdatadiagnosticsuggestion.updateuser

FROM
    {{ source("bay_dbo", "mdatadiagnosticsuggestion") }} AS mdatadiagnosticsuggestion
