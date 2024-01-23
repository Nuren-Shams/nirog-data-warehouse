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
            "refheadoffamily.createdate",
            "refheadoffamily.createuser",
            "refheadoffamily.description",
            "refheadoffamily.headoffamilycode",
            "refheadoffamily.headoffamilyid",
            "refheadoffamily.orgid",
            "refheadoffamily.sortorder",
            "refheadoffamily.status",
            "refheadoffamily.updatedate",
            "refheadoffamily.updateuser"
        ])
    }} AS ingestion_sk,
    refheadoffamily.createdate,
    refheadoffamily.createuser,
    refheadoffamily.description,
    refheadoffamily.headoffamilycode,
    refheadoffamily.headoffamilyid,
    refheadoffamily.orgid,
    refheadoffamily.sortorder,
    refheadoffamily.status,
    refheadoffamily.updatedate,
    refheadoffamily.updateuser

FROM
    {{ source("bay_dbo", "refheadoffamily") }} AS refheadoffamily
