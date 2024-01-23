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
            "refdesignation.createdate",
            "refdesignation.createuser",
            "refdesignation.description",
            "refdesignation.designationtitle",
            "refdesignation.orgid",
            "refdesignation.refdepartmentid",
            "refdesignation.refdesignationid",
            "refdesignation.sortorder",
            "refdesignation.status",
            "refdesignation.updatedate",
            "refdesignation.updateuser",
            "refdesignation.workplaceid"
        ])
    }} AS ingestion_sk,
    refdesignation.createdate,
    refdesignation.createuser,
    refdesignation.description,
    refdesignation.designationtitle,
    refdesignation.orgid,
    refdesignation.refdepartmentid,
    refdesignation.refdesignationid,
    refdesignation.sortorder,
    refdesignation.status,
    refdesignation.updatedate,
    refdesignation.updateuser,
    refdesignation.workplaceid

FROM
    {{ source("bay_dbo", "refdesignation") }} AS refdesignation
