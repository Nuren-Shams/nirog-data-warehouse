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
            "autosuggestion",
            "createdate",
            "createuser",
            "description",
            "orgid",
            "refautosuggestiongroupid",
            "refautosuggestionid",
            "sortorder",
            "status",
            "updatedate",
            "updateuser"
        ])
    }} AS ingestion_sk,
    autosuggestion,
    createdate,
    createuser,
    description,
    orgid,
    refautosuggestiongroupid,
    refautosuggestionid,
    sortorder,
    status,
    updatedate,
    updateuser

FROM
    {{ source("bay_dbo", "refautosuggestion") }}
