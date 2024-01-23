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
            "mdatapatientcervicalcancer.ccscreeningdiagnosis",
            "mdatapatientcervicalcancer.ccscreeningresultstatus",
            "mdatapatientcervicalcancer.collectiondate",
            "mdatapatientcervicalcancer.createdate",
            "mdatapatientcervicalcancer.createuser",
            "mdatapatientcervicalcancer.mdatapatientcervicalcancerid",
            "mdatapatientcervicalcancer.orgid",
            "mdatapatientcervicalcancer.patientid",
            "mdatapatientcervicalcancer.referralbiopsystatus",
            "mdatapatientcervicalcancer.referralbiopsystatusyescomments",
            "mdatapatientcervicalcancer.status",
            "mdatapatientcervicalcancer.thermocoagulationstatus",
            "mdatapatientcervicalcancer.thermocoagulationstatusno",
            "mdatapatientcervicalcancer.thermocoagulationstatusnoothercomments",
            "mdatapatientcervicalcancer.updatedate",
            "mdatapatientcervicalcancer.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientcervicalcancer.ccscreeningdiagnosis,
    mdatapatientcervicalcancer.ccscreeningresultstatus,
    mdatapatientcervicalcancer.collectiondate,
    mdatapatientcervicalcancer.createdate,
    mdatapatientcervicalcancer.createuser,
    mdatapatientcervicalcancer.mdatapatientcervicalcancerid,
    mdatapatientcervicalcancer.orgid,
    mdatapatientcervicalcancer.patientid,
    mdatapatientcervicalcancer.referralbiopsystatus,
    mdatapatientcervicalcancer.referralbiopsystatusyescomments,
    mdatapatientcervicalcancer.status,
    mdatapatientcervicalcancer.thermocoagulationstatus,
    mdatapatientcervicalcancer.thermocoagulationstatusno,
    mdatapatientcervicalcancer.thermocoagulationstatusnoothercomments,
    mdatapatientcervicalcancer.updatedate,
    mdatapatientcervicalcancer.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientcervicalcancer") }} AS mdatapatientcervicalcancer
