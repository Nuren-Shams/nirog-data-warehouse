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
            "createdate",
            "createuser",
            "mapicd10todiseasegroupid",
            "orgid",
            "refdiseasegroupid",
            "refprovisionaldiagnosisid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    mapicd10todiseasegroupid,
    orgid,
    refdiseasegroupid,
    refprovisionaldiagnosisid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mapicd10todiseasegroup") }}
