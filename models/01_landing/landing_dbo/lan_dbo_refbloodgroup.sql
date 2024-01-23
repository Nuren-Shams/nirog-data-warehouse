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
            "refbloodgroup.bloodgroupcode",
            "refbloodgroup.createdate",
            "refbloodgroup.createuser",
            "refbloodgroup.description",
            "refbloodgroup.orgid",
            "refbloodgroup.refbloodgroupid",
            "refbloodgroup.sortorder",
            "refbloodgroup.status",
            "refbloodgroup.updatedate",
            "refbloodgroup.updateuser"
        ])
    }} AS ingestion_sk,
    refbloodgroup.bloodgroupcode,
    refbloodgroup.createdate,
    refbloodgroup.createuser,
    refbloodgroup.description,
    refbloodgroup.orgid,
    refbloodgroup.refbloodgroupid,
    refbloodgroup.sortorder,
    refbloodgroup.status,
    refbloodgroup.updatedate,
    refbloodgroup.updateuser

FROM
    {{ source("bay_dbo", "refbloodgroup") }} AS refbloodgroup
