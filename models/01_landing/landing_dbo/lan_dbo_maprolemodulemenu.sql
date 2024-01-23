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
            "maprolemodulemenu.mappingrolemoduleid",
            "maprolemodulemenu.menuid",
            "maprolemodulemenu.moduleid",
            "maprolemodulemenu.orgid",
            "maprolemodulemenu.roleid",
            "maprolemodulemenu.status"
        ])
    }} AS ingestion_sk,
    maprolemodulemenu.mappingrolemoduleid,
    maprolemodulemenu.menuid,
    maprolemodulemenu.moduleid,
    maprolemodulemenu.orgid,
    maprolemodulemenu.roleid,
    maprolemodulemenu.status

FROM
    {{ source("bay_dbo", "maprolemodulemenu") }} AS maprolemodulemenu
