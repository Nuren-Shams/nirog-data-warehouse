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
            "module_id",
            "name",
            "slug",
            "updated_at"
        ])
    }} AS ingestion_sk,
    created_at,
    id,
    module_id,
    name,
    slug,
    updated_at

FROM
    {{ source("bay_dbo", "permissions") }}
