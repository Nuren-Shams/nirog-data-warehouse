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
            "deviceregistrations.created_at",
            "deviceregistrations.created_by",
            "deviceregistrations.deviceid",
            "deviceregistrations.id",
            "deviceregistrations.name",
            "deviceregistrations.status",
            "deviceregistrations.updated_at",
            "deviceregistrations.updated_by"
        ])
    }} AS ingestion_sk,
    deviceregistrations.created_at,
    deviceregistrations.created_by,
    deviceregistrations.deviceid,
    deviceregistrations.id,
    deviceregistrations.name,
    deviceregistrations.status,
    deviceregistrations.updated_at,
    deviceregistrations.updated_by

FROM
    {{ source("bay_dbo", "deviceregistrations") }} AS deviceregistrations
