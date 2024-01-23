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
            "divisions.bn_name",
            "divisions.id",
            "divisions.name",
            "divisions.url"
        ])
    }} AS ingestion_sk,
    divisions.bn_name,
    divisions.id,
    divisions.name,
    divisions.url

FROM
    {{ source("bay_dbo", "divisions") }} AS divisions
