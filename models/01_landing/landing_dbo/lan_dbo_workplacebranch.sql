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
            "workplacebranch.contactpersonname",
            "workplacebranch.createdate",
            "workplacebranch.createuser",
            "workplacebranch.description",
            "workplacebranch.email",
            "workplacebranch.isheadoffice",
            "workplacebranch.latitude",
            "workplacebranch.longitude",
            "workplacebranch.orgid",
            "workplacebranch.phone",
            "workplacebranch.sortorder",
            "workplacebranch.status",
            "workplacebranch.updatedate",
            "workplacebranch.updateuser",
            "workplacebranch.workplacebranchcode",
            "workplacebranch.workplacebranchid",
            "workplacebranch.workplaceid"
        ])
    }} AS ingestion_sk,
    workplacebranch.contactpersonname,
    workplacebranch.createdate,
    workplacebranch.createuser,
    workplacebranch.description,
    workplacebranch.email,
    workplacebranch.isheadoffice,
    workplacebranch.latitude,
    workplacebranch.longitude,
    workplacebranch.orgid,
    workplacebranch.phone,
    workplacebranch.sortorder,
    workplacebranch.status,
    workplacebranch.updatedate,
    workplacebranch.updateuser,
    workplacebranch.workplacebranchcode,
    workplacebranch.workplacebranchid,
    workplacebranch.workplaceid

FROM
    {{ source("bay_dbo", "workplacebranch") }} AS workplacebranch
