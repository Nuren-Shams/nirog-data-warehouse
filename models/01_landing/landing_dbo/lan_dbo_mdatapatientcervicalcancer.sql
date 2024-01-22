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
            "ccscreeningdiagnosis",
            "ccscreeningresultstatus",
            "collectiondate",
            "createdate",
            "createuser",
            "mdatapatientcervicalcancerid",
            "orgid",
            "patientid",
            "referralbiopsystatus",
            "referralbiopsystatusyescomments",
            "status",
            "thermocoagulationstatus",
            "thermocoagulationstatusno",
            "thermocoagulationstatusnoothercomments",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    ccscreeningdiagnosis,
    ccscreeningresultstatus,
    collectiondate,
    createdate,
    createuser,
    mdatapatientcervicalcancerid,
    orgid,
    patientid,
    referralbiopsystatus,
    referralbiopsystatusyescomments,
    status,
    thermocoagulationstatus,
    thermocoagulationstatusno,
    thermocoagulationstatusnoothercomments,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatapatientcervicalcancer") }}
