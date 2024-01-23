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
            "patients.created_at",
            "patients.created_by",
            "patients.id",
            "patients.name",
            "patients.status",
            "patients.updated_at",
            "patients.updated_by"
        ])
    }} AS ingestion_sk,
    patients.created_at,
    patients.created_by,
    patients.id,
    patients.name,
    patients.status,
    patients.updated_at,
    patients.updated_by

FROM
    {{ source("bay_dbo", "patients") }} AS patients
