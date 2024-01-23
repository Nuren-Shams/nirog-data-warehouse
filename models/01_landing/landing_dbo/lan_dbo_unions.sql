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
            "unions.bn_name",
            "unions.id",
            "unions.name",
            "unions.upazilla_id",
            "unions.url"
        ])
    }} AS ingestion_sk,
    unions.bn_name,
    unions.id,
    unions.name,
    unions.upazilla_id,
    unions.url

FROM
    {{ source("bay_dbo", "unions") }} AS unions
