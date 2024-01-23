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
            "mdatapatientillnesshistory.collectiondate",
            "mdatapatientillnesshistory.createdate",
            "mdatapatientillnesshistory.createuser",
            "mdatapatientillnesshistory.illnessid",
            "mdatapatientillnesshistory.mdpatientillnessid",
            "mdatapatientillnesshistory.orgid",
            "mdatapatientillnesshistory.otherillness",
            "mdatapatientillnesshistory.patientid",
            "mdatapatientillnesshistory.status",
            "mdatapatientillnesshistory.updatedate",
            "mdatapatientillnesshistory.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientillnesshistory.collectiondate,
    mdatapatientillnesshistory.createdate,
    mdatapatientillnesshistory.createuser,
    mdatapatientillnesshistory.illnessid,
    mdatapatientillnesshistory.mdpatientillnessid,
    mdatapatientillnesshistory.orgid,
    mdatapatientillnesshistory.otherillness,
    mdatapatientillnesshistory.patientid,
    mdatapatientillnesshistory.status,
    mdatapatientillnesshistory.updatedate,
    mdatapatientillnesshistory.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientillnesshistory") }} AS mdatapatientillnesshistory
