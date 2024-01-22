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
            "actualdate",
            "advice",
            "collectiondate",
            "comment",
            "createdate",
            "createuser",
            "mdpatientnewvaccineid",
            "orgid",
            "other",
            "patientid",
            "status",
            "suggesteddate",
            "updatedate",
            "updateuser",
            "vaccinedoseid",
            "vaccineid"
        ])
    }} AS ingestion_sk,
    actualdate,
    advice,
    collectiondate,
    comment,
    createdate,
    createuser,
    mdpatientnewvaccineid,
    orgid,
    other,
    patientid,
    status,
    suggesteddate,
    updatedate,
    updateuser,
    vaccinedoseid,
    vaccineid

FROM
    {{ source("bay_dbo", "mdatapatientnewvaccine") }}
