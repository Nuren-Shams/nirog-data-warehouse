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
            "reflabinvestigation.createdate",
            "reflabinvestigation.createuser",
            "reflabinvestigation.description",
            "reflabinvestigation.investigation",
            "reflabinvestigation.orgid",
            "reflabinvestigation.reflabinvestigationcode",
            "reflabinvestigation.reflabinvestigationgroupid",
            "reflabinvestigation.reflabinvestigationid",
            "reflabinvestigation.sortorder",
            "reflabinvestigation.status",
            "reflabinvestigation.updatedate",
            "reflabinvestigation.updateuser"
        ])
    }} AS ingestion_sk,
    reflabinvestigation.createdate,
    reflabinvestigation.createuser,
    reflabinvestigation.description,
    reflabinvestigation.investigation,
    reflabinvestigation.orgid,
    reflabinvestigation.reflabinvestigationcode,
    reflabinvestigation.reflabinvestigationgroupid,
    reflabinvestigation.reflabinvestigationid,
    reflabinvestigation.sortorder,
    reflabinvestigation.status,
    reflabinvestigation.updatedate,
    reflabinvestigation.updateuser

FROM
    {{ source("bay_dbo", "reflabinvestigation") }} AS reflabinvestigation
