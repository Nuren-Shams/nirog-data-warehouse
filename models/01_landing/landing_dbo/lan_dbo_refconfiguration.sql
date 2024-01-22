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
            "configid",
            "configkey",
            "configvalue",
            "createdate",
            "createuser",
            "description",
            "orgid",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    configid,
    configkey,
    configvalue,
    createdate,
    createuser,
    description,
    orgid,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "refconfiguration") }}
