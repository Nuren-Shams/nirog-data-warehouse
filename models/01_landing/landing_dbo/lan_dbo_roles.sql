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
            "roles.created_at",
            "roles.deletable",
            "roles.id",
            "roles.role_name",
            "roles.updated_at"
        ])
    }} AS ingestion_sk,
    roles.created_at,
    roles.deletable,
    roles.id,
    roles.role_name,
    roles.updated_at

FROM
    {{ source("bay_dbo", "roles") }} AS roles
