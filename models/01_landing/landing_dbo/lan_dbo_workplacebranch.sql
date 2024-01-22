{{
    config(
        materialized = "incremental",
        unique_key = "ingestion_sk",
        tags = ["execute_daily"]
    )
}}


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "contactpersonname",
            "createdate",
            "createuser",
            "description",
            "email",
            "isheadoffice",
            "latitude",
            "longitude",
            "orgid",
            "phone",
            "sortorder",
            "status",
            "updatedate",
            "updateuser",
            "workplacebranchcode",
            "workplacebranchid",
            "workplaceid"
        ])
    }} AS ingestion_sk,
    contactpersonname,
    createdate,
    createuser,
    description,
    email,
    isheadoffice,
    latitude,
    longitude,
    orgid,
    phone,
    sortorder,
    status,
    updatedate,
    updateuser,
    workplacebranchcode,
    workplacebranchid,
    workplaceid

FROM
    {{ source("bay_dbo", "workplacebranch") }}
