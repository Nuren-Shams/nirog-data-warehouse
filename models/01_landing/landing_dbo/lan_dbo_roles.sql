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
            "created_at",
            "deletable",
            "id",
            "role_name",
            "updated_at"
        ])
    }} AS ingestion_sk,
    created_at,
    deletable,
    id,
    role_name,
    updated_at

FROM
    {{ source("bay_dbo", "roles") }}
