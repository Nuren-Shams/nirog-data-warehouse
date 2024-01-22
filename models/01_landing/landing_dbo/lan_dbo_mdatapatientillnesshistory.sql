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
            "createdate",
            "createuser",
            "illnessid",
            "mdpatientillnessid",
            "orgid",
            "otherillness",
            "patientid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    collectiondate,
    createdate,
    createuser,
    illnessid,
    mdpatientillnessid,
    orgid,
    otherillness,
    patientid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatapatientillnesshistory") }}
