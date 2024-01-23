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
            "reffiletype.createdate",
            "reffiletype.createuser",
            "reffiletype.description",
            "reffiletype.filetypecode",
            "reffiletype.filetypeid",
            "reffiletype.orgid",
            "reffiletype.sortorder",
            "reffiletype.status",
            "reffiletype.updatedate",
            "reffiletype.updateuser"
        ])
    }} AS ingestion_sk,
    reffiletype.createdate,
    reffiletype.createuser,
    reffiletype.description,
    reffiletype.filetypecode,
    reffiletype.filetypeid,
    reffiletype.orgid,
    reffiletype.sortorder,
    reffiletype.status,
    reffiletype.updatedate,
    reffiletype.updateuser

FROM
    {{ source("bay_dbo", "reffiletype") }} AS reffiletype
