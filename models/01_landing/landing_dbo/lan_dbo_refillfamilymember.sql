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
            "refillfamilymember.createdate",
            "refillfamilymember.createuser",
            "refillfamilymember.description",
            "refillfamilymember.illfamilymembercode",
            "refillfamilymember.illfamilymemberid",
            "refillfamilymember.orgid",
            "refillfamilymember.sortorder",
            "refillfamilymember.status",
            "refillfamilymember.updatedate",
            "refillfamilymember.updateuser"
        ])
    }} AS ingestion_sk,
    refillfamilymember.createdate,
    refillfamilymember.createuser,
    refillfamilymember.description,
    refillfamilymember.illfamilymembercode,
    refillfamilymember.illfamilymemberid,
    refillfamilymember.orgid,
    refillfamilymember.sortorder,
    refillfamilymember.status,
    refillfamilymember.updatedate,
    refillfamilymember.updateuser

FROM
    {{ source("bay_dbo", "refillfamilymember") }} AS refillfamilymember
