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
            "id",
            "name",
            "updated_at",
            "value"
        ])
    }} AS ingestion_sk,
    created_at,
    id,
    name,
    updated_at,
    value

FROM
    {{ source("bay_dbo", "settings") }}
