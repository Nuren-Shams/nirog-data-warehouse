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
            "menus.created_at",
            "menus.deletable",
            "menus.id",
            "menus.menu_name",
            "menus.updated_at"
        ])
    }} AS ingestion_sk,
    menus.created_at,
    menus.deletable,
    menus.id,
    menus.menu_name,
    menus.updated_at

FROM
    {{ source("bay_dbo", "menus") }} AS menus
