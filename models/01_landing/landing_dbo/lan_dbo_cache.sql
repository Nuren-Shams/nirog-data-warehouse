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
            "expiration",
            "key",
            "value"
        ])
    }} AS ingestion_sk,
    expiration,
    key,
    value

FROM
    {{ source("bay_dbo", "cache") }}
