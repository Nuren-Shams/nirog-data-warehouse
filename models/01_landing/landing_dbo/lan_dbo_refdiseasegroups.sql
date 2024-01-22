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
            "diseasegroupname",
            "orgid",
            "refdiseasegroupid",
            "sortorder",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    createdate,
    createuser,
    diseasegroupname,
    orgid,
    refdiseasegroupid,
    sortorder,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "refdiseasegroups") }}
