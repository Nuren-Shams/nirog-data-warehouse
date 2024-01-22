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
            "connection",
            "exception",
            "failed_at",
            "id",
            "payload",
            "queue"
        ])
    }} AS ingestion_sk,
    connection,
    exception,
    failed_at,
    id,
    payload,
    queue

FROM
    {{ source("bay_dbo", "failed_jobs") }}
