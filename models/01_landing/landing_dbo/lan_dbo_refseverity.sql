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
            "refseverity.createdate",
            "refseverity.createuser",
            "refseverity.description",
            "refseverity.orgid",
            "refseverity.severitycode",
            "refseverity.severityid",
            "refseverity.severitylevel",
            "refseverity.sortorder",
            "refseverity.status",
            "refseverity.updatedate",
            "refseverity.updateuser"
        ])
    }} AS ingestion_sk,
    refseverity.createdate,
    refseverity.createuser,
    refseverity.description,
    refseverity.orgid,
    refseverity.severitycode,
    refseverity.severityid,
    refseverity.severitylevel,
    refseverity.sortorder,
    refseverity.status,
    refseverity.updatedate,
    refseverity.updateuser

FROM
    {{ source("bay_dbo", "refseverity") }} AS refseverity
