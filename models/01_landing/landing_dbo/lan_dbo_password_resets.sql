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
            "email",
            "token"
        ])
    }} AS ingestion_sk,
    created_at,
    email,
    token

FROM
    {{ source("bay_dbo", "password_resets") }}
