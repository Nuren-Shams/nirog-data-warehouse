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
            "category",
            "collectiondate",
            "createdate",
            "createuser",
            "diagnosisstatus",
            "diseasegroupname",
            "mdprovisionaldiagnosisid",
            "orgid",
            "otherprovisionaldiagnosis",
            "patientid",
            "provisionaldiagnosis",
            "refdiseasegroupid",
            "refprovisionaldiagnosisid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    category,
    collectiondate,
    createdate,
    createuser,
    diagnosisstatus,
    diseasegroupname,
    mdprovisionaldiagnosisid,
    orgid,
    otherprovisionaldiagnosis,
    patientid,
    provisionaldiagnosis,
    refdiseasegroupid,
    refprovisionaldiagnosisid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "mdataprovisionaldiagnosis") }}
