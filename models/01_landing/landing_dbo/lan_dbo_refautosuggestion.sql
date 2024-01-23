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
            "refautosuggestion.autosuggestion",
            "refautosuggestion.createdate",
            "refautosuggestion.createuser",
            "refautosuggestion.description",
            "refautosuggestion.orgid",
            "refautosuggestion.refautosuggestiongroupid",
            "refautosuggestion.refautosuggestionid",
            "refautosuggestion.sortorder",
            "refautosuggestion.status",
            "refautosuggestion.updatedate",
            "refautosuggestion.updateuser"
        ])
    }} AS ingestion_sk,
    refautosuggestion.autosuggestion,
    refautosuggestion.createdate,
    refautosuggestion.createuser,
    refautosuggestion.description,
    refautosuggestion.orgid,
    refautosuggestion.refautosuggestiongroupid,
    refautosuggestion.refautosuggestionid,
    refautosuggestion.sortorder,
    refautosuggestion.status,
    refautosuggestion.updatedate,
    refautosuggestion.updateuser

FROM
    {{ source("bay_dbo", "refautosuggestion") }} AS refautosuggestion
