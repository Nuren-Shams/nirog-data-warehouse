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
            "followupdate",
            "ispulled",
            "orgid",
            "patientid",
            "smsmdfollowupdateid"
        ])
    }} AS ingestion_sk,
    collectiondate,
    createdate,
    followupdate,
    ispulled,
    orgid,
    patientid,
    smsmdfollowupdateid

FROM
    {{ source("bay_dbo", "smsmdatafollowupdate") }}
