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
            "cache.expiration",
            "cache.key",
            "cache.value"
        ])
    }} AS ingestion_sk,
    cache.expiration,
    cache.key,
    cache.value

FROM
    {{ source("bay_dbo", "cache") }} AS cache
