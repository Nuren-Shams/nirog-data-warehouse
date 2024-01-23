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
            "migrations.batch",
            "migrations.id",
            "migrations.migration"
        ])
    }} AS ingestion_sk,
    migrations.batch,
    migrations.id,
    migrations.migration

FROM
    {{ source("bay_dbo", "migrations") }} AS migrations
