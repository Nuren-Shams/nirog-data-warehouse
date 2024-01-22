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
            "description",
            "districtid",
            "orgid",
            "status",
            "updatedate",
            "updateuser",
            "workplacecode",
            "workplaceid",
            "workplacename"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    description,
    districtid,
    orgid,
    status,
    updatedate,
    updateuser,
    workplacecode,
    workplaceid,
    workplacename

FROM
    {{ source("bay_dbo", "workplace") }}
