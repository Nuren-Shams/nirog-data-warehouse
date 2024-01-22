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
            "mappingworkplacedepartmentid",
            "orgid",
            "refdepartmentid",
            "status",
            "updatedate",
            "updateuser",
            "workplaceid"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    mappingworkplacedepartmentid,
    orgid,
    refdepartmentid,
    status,
    updatedate,
    updateuser,
    workplaceid

FROM
    {{ source("bay_dbo", "mapworkplacedepartment") }}
