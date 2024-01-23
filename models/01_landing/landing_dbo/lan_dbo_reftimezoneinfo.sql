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
            "reftimezoneinfo.current_utc_offset",
            "reftimezoneinfo.is_currently_dst",
            "reftimezoneinfo.name"
        ])
    }} AS ingestion_sk,
    reftimezoneinfo.current_utc_offset,
    reftimezoneinfo.is_currently_dst,
    reftimezoneinfo.name

FROM
    {{ source("bay_dbo", "reftimezoneinfo") }} AS reftimezoneinfo
