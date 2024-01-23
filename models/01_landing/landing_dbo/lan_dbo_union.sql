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
            "union.createdate",
            "union.createuser",
            "union.id",
            "union.orgid",
            "union.shortname",
            "union.status",
            "union.unionname",
            "union.upazilaid",
            "union.updatedate",
            "union.updateuser"
        ])
    }} AS ingestion_sk,
    union.createdate,
    union.createuser,
    union.id,
    union.orgid,
    union.shortname,
    union.status,
    union.unionname,
    union.upazilaid,
    union.updatedate,
    union.updateuser

FROM
    {{ source("bay_dbo", "union") }} AS union
