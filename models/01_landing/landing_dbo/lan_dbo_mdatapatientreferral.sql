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
            "healthcenterid",
            "mdpatientreferralid",
            "orgid",
            "patientid",
            "rid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    collectiondate,
    createdate,
    createuser,
    healthcenterid,
    mdpatientreferralid,
    orgid,
    patientid,
    rid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdatapatientreferral") }}
