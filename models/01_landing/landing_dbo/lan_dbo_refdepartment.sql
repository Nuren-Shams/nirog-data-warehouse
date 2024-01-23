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
            "refdepartment.createdate",
            "refdepartment.createuser",
            "refdepartment.departmentcode",
            "refdepartment.description",
            "refdepartment.orgid",
            "refdepartment.refdepartmentid",
            "refdepartment.sortorder",
            "refdepartment.status",
            "refdepartment.updatedate",
            "refdepartment.updateuser",
            "refdepartment.workplaceid"
        ])
    }} AS ingestion_sk,
    refdepartment.createdate,
    refdepartment.createuser,
    refdepartment.departmentcode,
    refdepartment.description,
    refdepartment.orgid,
    refdepartment.refdepartmentid,
    refdepartment.sortorder,
    refdepartment.status,
    refdepartment.updatedate,
    refdepartment.updateuser,
    refdepartment.workplaceid

FROM
    {{ source("bay_dbo", "refdepartment") }} AS refdepartment
