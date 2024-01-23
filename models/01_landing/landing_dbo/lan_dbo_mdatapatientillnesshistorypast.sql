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
            "mdatapatientillnesshistorypast.collectiondate",
            "mdatapatientillnesshistorypast.createdate",
            "mdatapatientillnesshistorypast.createuser",
            "mdatapatientillnesshistorypast.illnessid",
            "mdatapatientillnesshistorypast.mdpatientillnessid",
            "mdatapatientillnesshistorypast.orgid",
            "mdatapatientillnesshistorypast.otherillness",
            "mdatapatientillnesshistorypast.patientid",
            "mdatapatientillnesshistorypast.status",
            "mdatapatientillnesshistorypast.updatedate",
            "mdatapatientillnesshistorypast.updateuser"
        ])
    }} AS ingestion_sk,
    mdatapatientillnesshistorypast.collectiondate,
    mdatapatientillnesshistorypast.createdate,
    mdatapatientillnesshistorypast.createuser,
    mdatapatientillnesshistorypast.illnessid,
    mdatapatientillnesshistorypast.mdpatientillnessid,
    mdatapatientillnesshistorypast.orgid,
    mdatapatientillnesshistorypast.otherillness,
    mdatapatientillnesshistorypast.patientid,
    mdatapatientillnesshistorypast.status,
    mdatapatientillnesshistorypast.updatedate,
    mdatapatientillnesshistorypast.updateuser

FROM
    {{ source("bay_dbo", "mdatapatientillnesshistorypast") }} AS mdatapatientillnesshistorypast
