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
            "createdate",
            "createuser",
            "mappingworkplacebranch",
            "orgid",
            "status",
            "updatedate",
            "updateuser",
            "workplacebranchid",
            "workplaceid"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    mappingworkplacebranch,
    orgid,
    status,
    updatedate,
    updateuser,
    workplacebranchid,
    workplaceid

FROM
    {{ source("bay_dbo", "mapworkplacebranch") }}
