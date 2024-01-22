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
            "mappingrolemoduleid",
            "menuid",
            "moduleid",
            "orgid",
            "roleid",
            "status"
        ])
    }} AS ingestion_sk,
    mappingrolemoduleid,
    menuid,
    moduleid,
    orgid,
    roleid,
    status

FROM
    {{ source("bay_dbo", "maprolemodulemenu") }}
