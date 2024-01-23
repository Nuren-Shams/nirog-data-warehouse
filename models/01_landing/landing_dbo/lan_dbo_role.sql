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
            "role.createdate",
            "role.createuser",
            "role.description",
            "role.orgid",
            "role.rolecode",
            "role.roleid",
            "role.sortorder",
            "role.status",
            "role.updatedate",
            "role.updateuser"
        ])
    }} AS ingestion_sk,
    role.createdate,
    role.createuser,
    role.description,
    role.orgid,
    role.rolecode,
    role.roleid,
    role.sortorder,
    role.status,
    role.updatedate,
    role.updateuser

FROM
    {{ source("bay_dbo", "role") }} AS role
