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
            "refreligion.createdate",
            "refreligion.createuser",
            "refreligion.description",
            "refreligion.orgid",
            "refreligion.religioncode",
            "refreligion.religionid",
            "refreligion.sortorder",
            "refreligion.status",
            "refreligion.updatedate",
            "refreligion.updateuser"
        ])
    }} AS ingestion_sk,
    refreligion.createdate,
    refreligion.createuser,
    refreligion.description,
    refreligion.orgid,
    refreligion.religioncode,
    refreligion.religionid,
    refreligion.sortorder,
    refreligion.status,
    refreligion.updatedate,
    refreligion.updateuser

FROM
    {{ source("bay_dbo", "refreligion") }} AS refreligion
