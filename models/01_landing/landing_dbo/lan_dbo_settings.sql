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
            "settings.created_at",
            "settings.id",
            "settings.name",
            "settings.updated_at",
            "settings.value"
        ])
    }} AS ingestion_sk,
    settings.created_at,
    settings.id,
    settings.name,
    settings.updated_at,
    settings.value

FROM
    {{ source("bay_dbo", "settings") }} AS settings
