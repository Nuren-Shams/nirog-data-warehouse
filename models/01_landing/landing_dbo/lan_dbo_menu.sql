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
            "menu.createdate",
            "menu.createuser",
            "menu.description",
            "menu.menucode",
            "menu.menuid",
            "menu.moduleid",
            "menu.mothermenuid",
            "menu.orgid",
            "menu.sortorder",
            "menu.status",
            "menu.updatedate",
            "menu.updateuser"
        ])
    }} AS ingestion_sk,
    menu.createdate,
    menu.createuser,
    menu.description,
    menu.menucode,
    menu.menuid,
    menu.moduleid,
    menu.mothermenuid,
    menu.orgid,
    menu.sortorder,
    menu.status,
    menu.updatedate,
    menu.updateuser

FROM
    {{ source("bay_dbo", "menu") }} AS menu
