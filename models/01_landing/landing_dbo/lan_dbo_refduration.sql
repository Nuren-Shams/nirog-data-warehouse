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
            "refduration.createdate",
            "refduration.createuser",
            "refduration.description",
            "refduration.durationcode",
            "refduration.durationid",
            "refduration.durationinbangla",
            "refduration.durationinenglish",
            "refduration.orgid",
            "refduration.sortorder",
            "refduration.status",
            "refduration.updatedate",
            "refduration.updateuser"
        ])
    }} AS ingestion_sk,
    refduration.createdate,
    refduration.createuser,
    refduration.description,
    refduration.durationcode,
    refduration.durationid,
    refduration.durationinbangla,
    refduration.durationinenglish,
    refduration.orgid,
    refduration.sortorder,
    refduration.status,
    refduration.updatedate,
    refduration.updateuser

FROM
    {{ source("bay_dbo", "refduration") }} AS refduration
