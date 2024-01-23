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
            "refaddresstype.addresstypecode",
            "refaddresstype.addresstypeid",
            "refaddresstype.createdate",
            "refaddresstype.createuser",
            "refaddresstype.description",
            "refaddresstype.orgid",
            "refaddresstype.sortorder",
            "refaddresstype.status",
            "refaddresstype.updatedate",
            "refaddresstype.updateuser"
        ])
    }} AS ingestion_sk,
    refaddresstype.addresstypecode,
    refaddresstype.addresstypeid,
    refaddresstype.createdate,
    refaddresstype.createuser,
    refaddresstype.description,
    refaddresstype.orgid,
    refaddresstype.sortorder,
    refaddresstype.status,
    refaddresstype.updatedate,
    refaddresstype.updateuser

FROM
    {{ source("bay_dbo", "refaddresstype") }} AS refaddresstype
